package com.keevol.kvectors.repository

import com.keevol.kvectors.utils.{Encoding, With}
import io.vertx.core.json.{JsonArray, JsonObject}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.FileFilterUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.io.{File, IOException}
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.{ByteBuffer, ByteOrder}
import java.util
import java.util.{Collections, Comparator, ArrayList => JArrayList}
import scala.collection.mutable.ListBuffer

/**
 * @deprecated this is only referenced by deprecated [[com.keevol.kvectors.fs.MmapSegmentsRandomAccessVectorValues]], we will remove it later on.
 */
@Deprecated
object IndexConstants {
  val indexSnapshotJournalListFilename: String = "index_vectors_snapshot.lst"
}

trait VecFileList {
  def listVecFilesWithOrder(dataDir: File, collectionName: String): util.ArrayList[String] = {
    val vectorFilenameList: JArrayList[String] = new JArrayList[String]()
    // list all .vec* files (including the being written one)
    FileUtils.listFiles(dataDir, FileFilterUtils.prefixFileFilter(s"${collectionName}.vec"), null).forEach(file => vectorFilenameList.add(file.getName))
    Collections.sort(vectorFilenameList, new Comparator[String] {
      override def compare(o1: String, o2: String): Int = (StringUtils.substringAfterLast(o1, ".").toLong - StringUtils.substringAfterLast(o2, ".").toLong).toInt
    }) // to ensure stable order
    vectorFilenameList
  }
}

/**
 * represents a section of snapshot, usually the whole file,
 *
 * but sometimes, part of a file is also allowed. e.g. the vec journal that's being written into.
 *
 * @param vecFile file which stores vectors
 * @param offset start of the section
 * @param size length of the section
 */
case class SnapshotSection(vecFile: File, offset: Long, size: Long) {
  def asJson(): JsonObject = JsonObject.of("file", vecFile.getAbsolutePath, "offset", offset, "size", size)
}

object SnapshotSection {
  def apply(json: JsonObject): SnapshotSection = {
    val file = json.getString("file")
    val offset = json.getLong("offset")
    val size = json.getLong("size")
    new SnapshotSection(new File(file), offset, size)
  }
}

/**
 * to hold a frozen snapshot which forms the base dataset of a new ANN index.
 *
 * invoked by ANN Vector Collection Impl.
 *
 * The result(snapshot sections) is for RAVV impl. to use.
 */
class IndexDataSetSnapshot(indexDir: File) extends VecFileList {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  require(indexDir.isDirectory, "the indexDir should be a directory.")
  private var lastVectorId: Long = -1
  private val sections: ListBuffer[SnapshotSection] = new ListBuffer[SnapshotSection]()
  //  private val snapshotResultFile = new File(indexDir, IndexConstants.indexSnapshotJournalListFilename)
  private val snapshotResultFile = new File(indexDir, "snapshot.json")
  // when index is built and will be queried, its dataset snapshot will be loaded before read via RAVV.
  if (snapshotResultFile.exists()) {
    val json = new JsonObject(FileUtils.readFileToString(snapshotResultFile, Encoding.default()))
    logger.info(s"load (${snapshotResultFile.getAbsolutePath}) with content: ${json.encode()}")
    json.getJsonArray("segments").forEach(jsonObject => {
      sections.append(SnapshotSection(jsonObject.asInstanceOf[JsonObject]))
    })
    lastVectorId = json.getLong("last_vector_id")
  }

  private def addSnapshotSection(section: SnapshotSection): Unit = {
    require(section != null)
    sections.append(section)
  }

  def getSnapshotSections: List[SnapshotSection] = sections.toList

  def getLastVectorId: Long = lastVectorId

  /**
   * list .vec files from data dir of the collection, but snapshot state file should be written to index dir.(that's snapshotTo file)
   *
   * This method should be called by vector collection implementation before a new index building.
   *
   * @param dataDir where .vec files store
   * @param collectionName vector collection name
   * @param frameLayout we need it to calculate frame size
   */
  def snapshot(dataDir: File, collectionName: String, frameLayout: VectorFrameLayout): Unit = {
    require(dataDir.isDirectory, s"Data directory '$dataDir' is not a valid directory.")

    val frameSize = frameLayout.frameSize
    require(frameSize > 0, "Frame size must be positive.")

    val vectorFilenameList: JArrayList[String] = listVecFilesWithOrder(dataDir, collectionName)
    val json = new JsonObject()
    val segmentArray = new JsonArray()
    (0 until vectorFilenameList.size()).foreach { i =>
      val filename = vectorFilenameList.get(i)
      val file = new File(dataDir, filename)
      val size = file.length()
      // if current file is the being written one, the size of it may not reflect a whole pack of complete vectors, so we have to check and validate it
      val limitSize = (size / frameSize) * frameSize
      val segment = SnapshotSection(file, 0, limitSize)
      addSnapshotSection(segment)
      segmentArray.add(segment.asJson())
      logger.info(s"add snapshot section(filename=${filename}, size=${size}, limit=${limitSize})")
    }
    json.put("segments", segmentArray)
    val lastSegment = getSnapshotSections.last
    val fc = FileChannel.open(lastSegment.vecFile.toPath, StandardOpenOption.READ)
    With(fc) {
      fc.position(lastSegment.size - frameSize)
      val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
      buffer.order(ByteOrder.BIG_ENDIAN)
      val bytesRead = fc.read(buffer)
      if (bytesRead != java.lang.Long.BYTES) {
        throw new IOException("Could not read a full long...");
      }
      buffer.flip()
      val lastVectorId = buffer.getLong
      json.put("last_vector_id", lastVectorId)
    }
    FileUtils.writeStringToFile(snapshotResultFile, json.encode(), Encoding.default(), false)
    logger.info(s"snapshot result: ${json.encodePrettily()}")
  }
}
