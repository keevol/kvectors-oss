package com.keevol.kvectors.repository

/**
 * <pre>
 * ██╗  ██╗ ███████╗ ███████╗ ██╗   ██╗  ██████╗  ██╗
 * ██║ ██╔╝ ██╔════╝ ██╔════╝ ██║   ██║ ██╔═══██╗ ██║
 * █████╔╝  █████╗   █████╗   ██║   ██║ ██║   ██║ ██║
 * ██╔═██╗  ██╔══╝   ██╔══╝   ╚██╗ ██╔╝ ██║   ██║ ██║
 * ██║  ██╗ ███████╗ ███████╗  ╚████╔╝  ╚██████╔╝ ███████╗
 * ╚═╝  ╚═╝ ╚══════╝ ╚══════╝   ╚═══╝    ╚═════╝  ╚══════╝
 * </pre>
 * <p>
 * KEEp eVOLution!
 * <p>
 *
 * @author fq@keevol.cn
 * @since 2017.5.12
 * <p>
 * Copyright 2017 © 杭州福强科技有限公司版权所有 (<a href="https://www.keevol.cn">keevol.cn</a>)
 */

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.keevol.goodies.threads.Threads
import com.keevol.kvectors.fs.ByteAndBuffer
import com.keevol.kvectors.utils.{Closables, Encoding, With}
import com.keevol.kvectors.{KVector, KVectorLite}
import io.vertx.core.json.JsonObject
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.FileFilterUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory

import java.io.{File, RandomAccessFile}
import java.lang.foreign.Arena
import java.lang.{Long => JLong}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption.{CREATE, READ, WRITE}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.{CopyOnWriteArrayList, LinkedBlockingQueue}
import java.util.{Collections, Comparator, ArrayList => JArrayList}
import java.{lang, util}
import scala.beans.BeanProperty
import scala.collection.JavaConverters._

/**
 * Vector store backed by local file system.
 *
 * @param dataDir collection store dir where to store the vectors
 * @param vectorCollectionName the name of vector collection
 */
class LocalFSVectorStore(dataDir: File, vectorCollectionName: String, loadAllVectorsToMemory: Boolean = true) extends KVectorStore {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  require(dataDir != null && StringUtils.isNotEmpty(vectorCollectionName))

  private val lastVectorId = new AtomicLong(0)
  //
  private val metaStore = new MapdbVectorMetaManager(new File(dataDir, vectorCollectionName + "-metastore.mdb"))

  // if loadAllVectorsToMemory = false, this will just be a ref without any content, otherwise, it will cache all vectors in memory.
  private val allVectors: Cache[JLong, KVectorLite] = Caffeine.newBuilder().build[JLong, KVectorLite]()

  private val activeFile = new AtomicReference[String]() // for runtime ref
  private val activeFileMetaFile = new File(dataDir, "active.vec.journal") // for save and load at start and stop
  private val activeChannel: AtomicReference[FileChannel] = new AtomicReference[FileChannel]() // for rollover replacement
  private val activeChannelBufferedVectors: Cache[JLong, KVector] = Caffeine.newBuilder().build()
  private val activeChannelBufferedVectorsVersion = new AtomicLong() // don't care the number, but only the diff, when iterator hold a diff version, throw illegal state exception at id out of range.

  // all journals in memory for easy access
  private val journals: CopyOnWriteArrayList[JournalRange] = new CopyOnWriteArrayList[JournalRange]()

  // attributes dynamically determined at runtime
  private val vectorFrameBuffer: AtomicReference[ByteBuffer] = new AtomicReference[ByteBuffer]()
  private val dimensionHolder: AtomicInteger = new AtomicInteger()
  private val dimensionMetaFile: File = new File(dataDir, vectorCollectionName + ".dim")
  private val frameLayout: AtomicReference[VectorFrameLayout] = new AtomicReference[VectorFrameLayout]()
  // async and conforms to Single-writer principle
  private val rv: LinkedBlockingQueue[(Long, Array[Float])] = new LinkedBlockingQueue[(Long, Array[Float])]()
  private val journalRunnable: Runnable = () => {
    while (running.get() || (!rv.isEmpty)) {
      val e = rv.poll()
      if (e != null) { // ManyToOneConcurrentLinkedQueue doesn't conform to convention, so the poll may return null as result.
        val (id, vector) = e
        try {
          val dimension = vector.length
          val vectorFrameLen = java.lang.Long.BYTES + java.lang.Float.BYTES * dimension // Long(8 bytes) + float(4 bytes) * vector dimension
          if (vectorFrameBuffer.get() == null) {
            val directBuffer = ByteBuffer.allocateDirect(vectorFrameLen)
            vectorFrameBuffer.set(directBuffer)
          }
          if (dimensionHolder.get() == 0) {
            dimensionHolder.set(dimension)
            if (!dimensionMetaFile.exists()) {
              FileUtils.writeStringToFile(dimensionMetaFile, dimension.toString, StandardCharsets.UTF_8, false)
              // since it's the first vector in collection, setup vector frame layout here.
              frameLayout.set(new VectorFrameLayout(dimension))
            }
          }
          require(vector.length == dimensionHolder.get(), s"vector dimension collision, expected:${dimensionHolder.get()}, get: ${vector.length}")
          logger.debug(s"write vector with id=${id} to journal...")
          val buffer = vectorFrameBuffer.get()
          buffer.clear()
          buffer.putLong(id)
          buffer.put(ByteAndBuffer.floatsToBytes(vector))
          buffer.flip()
          while (buffer.hasRemaining) {
            activeChannel.get().write(buffer)
          }

          // rollover if necessary
          if (activeChannel.get().size() > segmentSize) {
            // close last channel before rollover
            activeChannel.get().close()

            // create mmap for read & clear in-memory cache with it
            registerJournalRangeWith(getActiveFile(), Option(id))
            // clear in-memory state
            activeChannelBufferedVectorsVersion.getAndIncrement() // invalidate former iterator on vectors if any
            activeChannelBufferedVectors.invalidateAll(new lang.Iterable[JLong] {
              override def iterator(): util.Iterator[JLong] = new util.Iterator[JLong] {
                private val counter = new AtomicLong(id)

                override def hasNext: Boolean = counter.get() >= 0

                override def next(): JLong = counter.getAndDecrement()
              }
            })

            // do rollover
            val nextJnlFilename = nextJournalFilename()
            activeFile.set(nextJnlFilename)
            logger.info(s"rollover vector storage file to ${nextJnlFilename}")
            openActiveFileChannel()
          }
        } catch {
          case t: Throwable => {
            logger.error(s"fatal error on vector write: ${ExceptionUtils.getStackTrace(t)}")
            try {
              val writeErrorVectorDLQ = new File(dataDir, "dlq") // dead letter queue with local dir as store
              if (!writeErrorVectorDLQ.exists()) {
                FileUtils.forceMkdir(writeErrorVectorDLQ)
              }
              val raf = new RandomAccessFile(new File(writeErrorVectorDLQ, s"${id}.vector"), "rw")
              With(raf) {
                vector.foreach(raf.writeFloat)
              }
            } catch {
              case e: Throwable => logger.warn(s"fails to backup vector data with id = ${id}: \n${ExceptionUtils.getStackTrace(e)}")
            }
          }
        }
      }
    }
  }

  @BeanProperty
  var vectorJournalFileSuffixName = ".vec"

  @BeanProperty
  var segmentSize: Long = 1 << 30; // 1G as default which takes mmap limit into consideration.

  override protected def doStart(): Unit = {
    require(segmentSize < Integer.MAX_VALUE, "segmentSize must less than 2147483647 in case exceeding mmap limit")

    if (!dataDir.exists()) {
      FileUtils.forceMkdir(dataDir)
    }

    if (dimensionMetaFile.exists()) {
      // restore dimension related fields
      val dim = StringUtils.trimToEmpty(FileUtils.readFileToString(dimensionMetaFile, StandardCharsets.UTF_8))
      if (StringUtils.isNotEmpty(dim)) {
        val dimension = dim.toInt
        dimensionHolder.set(dimension)
        frameLayout.set(new VectorFrameLayout(dimension))
      }
    }


    val activeFileName = if (activeFileMetaFile.exists()) {
      FileUtils.readFileToString(activeFileMetaFile, Encoding.default())
    } else {
      nextJournalFilename()
    }
    activeFile.set(activeFileName)

    val journalFiles = listVecFilesWithOrder(dataDir = dataDir, collectionName = vectorCollectionName)
    journalFiles.forEach(f => registerJournalRangeWith(f))

    // kick off ingest
    openActiveFileChannel()

    // kick off journal writer
    Thread.ofVirtual()
      .uncaughtExceptionHandler(Threads.defaultUncaughtExceptionhandler())
      .name("LocalFSVectorStore Journal Writer Thread")
      .start(journalRunnable)
  }

  override protected def doStop(): Unit = {

    // --- save journal state
    if (vectorFrameBuffer.get() != null) {
      // as to direct buffer, we can do nothing except waiting for gc.
      vectorFrameBuffer.get().clear()
      vectorFrameBuffer.set(null)
    }
    /**
     * another way is to list file as per timestamp without save such meta file.
     */
    val activeFilename = activeFile.get()
    if (StringUtils.isNotEmpty(activeFilename)) {
      FileUtils.writeStringToFile(activeFileMetaFile, activeFilename, Encoding.default(), false)
    }

    if (activeChannel.get() != null) {
      activeChannel.get().close()
      activeChannel.set(null)
    }


    // release resources related to journals
    journals.forEach(e => {
      Closables.closeWithLog(e.arena)
      Closables.closeWithLog(e.fc)
    })
    // clear cache
    activeChannelBufferedVectors.invalidateAll()
    activeChannelBufferedVectors.cleanUp()
    // --- close meta store
    metaStore.close()

  }

  /**
   * save a vector to store
   *
   * @param vector to save
   */
  override def add(vector: KVector): Unit = {
    // save same vector to memory cache for partial access before next rollover
    activeChannelBufferedVectors.put(vector.id, vector)
    // async write in bg
    rv.put((vector.id, vector.vector))
    // separate store for meta
    metaStore.add(vector.id, JsonObject.of("rid", vector.rid, "meta", vector.meta))
    // mark last id
    lastVectorId.set(vector.id)

    if (loadAllVectorsToMemory) {
      allVectors.put(vector.id, KVectorLite(vector.id, vector.vector))
    }
  }

  /**
   * last vector id which helps us to determine an approximate count
   *
   * @return last vector id
   */
  override def last(): Long = lastVectorId.get()

  /**
   * get a full vector as per vector id
   *
   * @param id of vector
   * @return a full vector
   */
  override def get(id: Long): Option[KVector] = {
    value(id).map(kvl => {
      logger.debug(s"get meta of vector with id=${id}")
      val json = metaStore.get(kvl.id)
      val meta = json.getOrElse(JsonObject.of("rid", "[[UNEXPECTED]]"))
      KVector(kvl.id, kvl.value, meta.getString("rid"), StringUtils.trimToEmpty(meta.getString("meta")))
    })
  }

  /**
   * two-phase fetch:
   * 1. fetch from journal range if in the range
   * 2. fetch from in memory cache as fallback
   *
   * if the calculated expected position has unmatched id, seek before or after as per the id value at position.(粗略定位，加前后扫描)
   *
   * @param id vector id
   *  @return a vector with id and value
   */
  override def value(id: Long): Option[KVectorLite] = {
    // short-circuit
    if (loadAllVectorsToMemory) {
      return Option(allVectors.getIfPresent(id))
    }

    findJournalRangeWith(id) match {
      case Some(journalRange: JournalRange) =>
        logger.debug(s"try to locate vector details from journal range with limit id=${journalRange.lastId}")
        val segment = journalRange.memorySegment
        val lastId = journalRange.lastId
        val frame = frameLayout.get()
        val frameSize = frame.frameSize
        val frameCount = frame.frameCount(segment)

        // 1. O(1) 乐观定位计算
        val expectedOffset = (frameCount - (lastId - id) - 1) * frameSize

        // 边界情况：如果计算出的偏移量超出范围，说明id虽然小于lastId, 但不在本段内，这是一个罕见的异常
        if (expectedOffset < 0) {
          logger.error(s"计算出的偏移量为负数 (${expectedOffset})！id=${id}, lastId=${lastId}, frameCount=${frameCount}. 这可能表示数据不一致。")
          // 这里可以进行一次全段扫描作为最后的补救措施，或者直接返回None
          // 为了安全，我们选择返回None
          None
        } else {
          val targetId = frame.getId(segment, expectedOffset)

          targetId.compareTo(id) match {
            case 0 => // 完美命中！
              logger.info(s"乐观查找命中！id=${id} 位于偏移量=${expectedOffset}")
              Some(KVectorLite(targetId, frame.getVector(segment, expectedOffset)))

            case 1 => // Overshoot: 期望位置的ID > 查询ID
              logger.warn(s"乐观查找 overshoot：期望位置ID=${targetId}, 查询ID=${id}。开始向后（低地址）扫描...")
              val searchRange = (expectedOffset - frameSize to 0 by -frameSize).view // 使用 .view 可以确保在takeWhile和find的过程中不会创建任何中间集合，内存开销极小。
              val possibleOffsets = searchRange.takeWhile(offset => frame.getId(segment, offset) >= id)
              val foundOffset = possibleOffsets.find(offset => frame.getId(segment, offset) == id)

              foundOffset.map { offset =>
                logger.info(s"向后扫描成功，在偏移量=${offset} 处找到 id=${id}")
                KVectorLite(id, frame.getVector(segment, offset))
              }

            case -1 => // Undershoot: 期望位置的ID < 查询ID
              logger.warn(s"乐观查找 undershoot：期望位置ID=${targetId}, 查询ID=${id}。开始向前（高地址）扫描...")
              val lastFrameOffset = (frameCount - 1) * frameSize
              val searchRange = (expectedOffset + frameSize to lastFrameOffset by frameSize).view // 使用 .view 可以确保在takeWhile和find的过程中不会创建任何中间集合，内存开销极小。
              val possibleOffsets = searchRange.takeWhile(offset => frame.getId(segment, offset) <= id)
              val foundOffset = possibleOffsets.find(offset => frame.getId(segment, offset) == id)

              foundOffset.map { offset =>
                logger.info(s"向前扫描成功，在偏移量=${offset} 处找到 id=${id}")
                KVectorLite(id, frame.getVector(segment, offset))
              }
          }
        }

      case None =>
        logger.debug("can't find the vector in journals, search in memory buffer as fallback...")
        Option(activeChannelBufferedVectors.getIfPresent(id)).map(vec => KVectorLite(vec.id, vec.vector))

    }
  }

  /**
   * we resort to optimized state control on this method, so that scan and ls/all can emit correct data entry.
   *
   * so we override the implementation over [[KVectorStore]]
   */
  override def keys(): Iterator[Long] = new Iterator[Long] {
    private val counter = new AtomicLong(0)
    private val version = activeChannelBufferedVectorsVersion.get()

    override def hasNext: Boolean = counter.get() < last()

    override def next(): Long = {
      if (activeChannelBufferedVectorsVersion.get() != version) throw new IllegalStateException("storage journal changes")
      counter.getAndIncrement()
    }
  }

  private def findJournalRangeWith(id: Long): Option[JournalRange] = journals.asScala.find(journalRange => id <= journalRange.lastId)

  /**
   * 其实如果在metastore里存journal index， 这个方法里的实现就不用那么复杂了。
   */
  private def registerJournalRangeWith(file: File, idOpt: Option[Long] = None): Unit = {
    val journalFile = file
    if (journalFile.length() == 0) {
      logger.info("ignore empty journal file.")
      return
    }
    val fc = openReadOnlyFileChannel(journalFile)
    val arena = Arena.ofShared()
    val memorySegment = fc.map(MapMode.READ_ONLY, 0, journalFile.length(), arena)
    // if no id provided, we read the last frame of the segment to get it.
    val lastId = idOpt match {
      case Some(id) => id
      case None => {
        val layout = frameLayout.get()
        val count = layout.frameCount(memorySegment)
        val offset = (count - 1) * layout.frameSize
        layout.getId(memorySegment, offset)
      }
    }
    journals.add(JournalRange(journalFile, fc, arena, memorySegment, lastId))

    if (loadAllVectorsToMemory) {
      val layout = frameLayout.get()
      for (i <- 0 until layout.frameCount(memorySegment).toInt) {
        val offset = i * layout.frameSize
        val id = layout.getId(memorySegment, offset)
        val vector = layout.getVector(segment = memorySegment, frameOffset = offset)
        allVectors.put(id, KVectorLite(id, vector))
      }
    }
  }

  private def getActiveFile(): File = {
    val activeFilename = activeFile.get()
    require(StringUtils.isNotEmpty(activeFilename))
    new File(dataDir, activeFilename)
  }

  private def openReadOnlyFileChannel(file: File): FileChannel = FileChannel.open(file.toPath, READ)

  // create new file if non-exists, in our case, after rename or move old active file.
  private def openActiveFileChannel(): Unit = {
    activeChannel.set(FileChannel.open(getActiveFile().toPath, CREATE, READ, WRITE))
  }

  private def nextJournalFilename(): String = vectorCollectionName + vectorJournalFileSuffixName + "." + System.currentTimeMillis()

  private def listVecFilesWithOrder(dataDir: File, collectionName: String): util.List[File] = {
    val vectorFilenameList: JArrayList[String] = new JArrayList[String]()
    // list all .vec* files (including the being written one)
    FileUtils.listFiles(dataDir, FileFilterUtils.prefixFileFilter(s"${collectionName}.vec"), null).forEach(file => vectorFilenameList.add(file.getName))
    Collections.sort(vectorFilenameList, new Comparator[String] {
      override def compare(o1: String, o2: String): Int = (StringUtils.substringAfterLast(o1, ".").toLong - StringUtils.substringAfterLast(o2, ".").toLong).toInt
    }) // to ensure stable order
    vectorFilenameList.asScala.map((fn: String) => new File(dataDir, fn)).asJava
  }


  /**
   * get the dimension of vectors in store.
   *
   * @return vector dimension, 0 or real dimension
   */
  override def dimension: Int = dimensionHolder.get()
}