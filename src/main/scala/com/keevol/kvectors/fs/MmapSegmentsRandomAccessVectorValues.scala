package com.keevol.kvectors.fs

/**
 * <pre>
 * :::    ::: :::::::::: :::::::::: :::     :::  ::::::::  :::
 * :+:   :+:  :+:        :+:        :+:     :+: :+:    :+: :+:
 * +:+  +:+   +:+        +:+        +:+     +:+ +:+    +:+ +:+
 * +#++:++    +#++:++#   +#++:++#   +#+     +:+ +#+    +:+ +#+
 * +#+  +#+   +#+        +#+         +#+   +#+  +#+    +#+ +#+
 * #+#   #+#  #+#        #+#          #+#+#+#   #+#    #+# #+#
 * ###    ### ########## ##########     ###      ########  ##########
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

import com.keevol.kvectors.lifecycles.Lifecycle
import com.keevol.kvectors.repository.{IndexConstants, IndexDataSetSnapshot, SnapshotSection}
import com.keevol.kvectors.utils.{Closables, Encoding}
import io.github.jbellis.jvector.graph.RandomAccessVectorValues
import io.github.jbellis.jvector.vector.VectorizationProvider
import io.github.jbellis.jvector.vector.types.VectorFloat
import io.vertx.core.json.JsonObject
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.FileFilterUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption._
import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

/**
 * This is only for simplest implementation strategy.
 *
 * In next revision, we can resort to calculation with slots without storing so many references in memory.
 *
 * @deprecated turn to [[com.keevol.kvectors.repository.MemorySegmentRandomAccessVectorValues]] based impl. from FFM API
 * @param mappedBuffer where to fetch id from
 * @param position the start position of the id and frame in mappedBuffer
 */
@Deprecated
case class VectorFrameWrapper(mappedBuffer: MappedByteBuffer, position: Long)

/**
 * since not all .vec file has same size, let's say, the being written one.
 *
 * so we have to snapshot the building states so that everything is going to be immutable to serve.
 *
 * @see [[SnapshotSection]]
 * @param filename .vec file name
 * @param watermark the size to snapshot and freeze
 */
case class MmapSegment(filename: String, watermark: Long)

/**
 * @deprecated turn to MemorySegment based impl. from FFM API
 * @see [[IndexDataSetSnapshot]]
 */
@Deprecated
object MmapSegment {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def loadSegments(dataDir: File, indexDir: File, collectionName: String): ConcurrentHashMap[String, Long] = {
    val vectorJournalSnapshotFile = new File(indexDir, IndexConstants.indexSnapshotJournalListFilename)
    logger.info(s"load vector segments from ${vectorJournalSnapshotFile}")
    val segments = new ConcurrentHashMap[String, Long]()
    if (vectorJournalSnapshotFile.exists()) {
      FileUtils.readLines(vectorJournalSnapshotFile, Encoding.default()).forEach(line => {
        val filename = StringUtils.substringBefore(line, ",")
        val limitSize = StringUtils.substringAfter(line, ",").toLong
        logger.info(s"segment loaded: ${filename}, ${limitSize}")
        segments.put(filename, limitSize)
      })
    } else {
      logger.info("do nothing at loadSegments, we only care about the existing one when subtract with the whole list of .vec")
    }
    segments
  }

  def listVectorFiles(dataDir: File, collectionName: String) = FileUtils.listFiles(dataDir, FileFilterUtils.prefixFileFilter(s"${collectionName}.vec"), null)
}

/**
 * immutable after initialized.
 *
 * MmapSegmentsRandomAccessVectorValues only takes care of read, NO write.
 * @deprecated turn to MemorySegment based impl. from FFM API
 * @param dataDir where vector collection locates
 */
@Deprecated
class MmapSegmentsRandomAccessVectorValues(dataDir: File, indexDir: File, collectionName: String, dimension: Int, autoStart: Boolean = true) extends RandomAccessVectorValues with Lifecycle {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  private final val vts = VectorizationProvider.getInstance().getVectorTypeSupport
  private final val frameSize = java.lang.Long.BYTES + dimension * java.lang.Float.BYTES

  // 需要存储一个固定的journal list作为snapshot，从而固定索引数据集
  // if journals.snapshot exists, load them as per it, otherwise, load all and save a journal snapshot.
  private val indexSnapshotJournalListFilename: String = IndexConstants.indexSnapshotJournalListFilename
  private val vectorJournalSnapshotFile: File = new File(indexDir, indexSnapshotJournalListFilename)
  private val slots = new util.ArrayList[Mmap]() // snapshot is immutable, this make things easy.
  private val vectorsIndex: util.ArrayList[VectorFrameWrapper] = new util.ArrayList[VectorFrameWrapper]()

  if (autoStart) {
    start()
  }

  /**
   * Add all of the .vec journals with stable order and save not only their file name but also their size/cursor
   *
   * In this way, we can cover indexing data scope seamlessly.
   *
   * The 1st revision which only covers completed journal files had a fault that will not indexing vectors when only one vec journal exists.  that's, the being written one.
   */
  override protected def doStart(): Unit = {
    // load vectors from dataDir and reset states of size and dimension\
    val segments = if (vectorJournalSnapshotFile.exists()) {
      FileUtils.readLines(vectorJournalSnapshotFile, Encoding.default())
    } else {
      val list: util.ArrayList[String] = new util.ArrayList[String]()
      // list all .vec* files (including the being written one)
      MmapSegment.listVectorFiles(dataDir, collectionName).forEach(file => {
        list.add(file.getName)
      })
      Collections.sort(list) // to ensure stable order
      val sb = new java.lang.StringBuilder()
      (0 until list.size()).foreach { i =>
        val filename = list.get(i)
        val file = new File(dataDir, filename)
        val size = file.length()
        // if current file is the being written one, the size of it may not reflect a whole pack of complete vectors, so we have to check and validate it
        val limitSize = if (size % frameSize == 0) size else (size / frameSize) * frameSize
        sb.append(s"${filename},${limitSize}")
        sb.append("\n")
      }
      FileUtils.writeStringToFile(vectorJournalSnapshotFile, sb.toString, Encoding.default(), false)
      util.Arrays.asList(StringUtils.split(sb.toString, "\n"): _*)
    }
    // pre-read positons in each mmap to index them in an array or list(seq), so that when getVector is called, we can get there quickly.
    // ONLY calculate the position, NO io !!!
    // since we know the frame size.
    // after the position and its mmap ref is added to a sequence, we can refer to them anytime at ordinal access.
    (0 until segments.size()).foreach(i => {
      val filename = StringUtils.substringBefore(segments.get(i), ",")
      val limitSize = StringUtils.substringAfter(segments.get(i), ",").toLong
      openFileChannelAndCreateMmapFor(new File(dataDir, filename), limitSize)
    })
  }

  private def openFileChannelAndCreateMmapFor(file: File, limitSize: Long): Unit = {
    require(limitSize <= Int.MaxValue, "File size exceeds 2GB limit")

    val fc = FileChannel.open(file.toPath, READ)
    val mappedBuffer = fc.map(MapMode.READ_ONLY, 0, limitSize)
    slots.add(Mmap(fc, mappedBuffer, file))
    var i = 0L
    while (i < limitSize) {
      vectorsIndex.add(VectorFrameWrapper(mappedBuffer, i))
      i = i + frameSize
    }
  }

  /**
   * RAVV doesn't fucking care about the real vector id , they only care about the floats and its ord in some sequence.
   *
   * @param i ordinal(序数词) in RAVV, it will come from SearchResult.NodeScore.node(Int), another field of SearchResult.NodeScore is score.
   * @return the real vector Id in our own way.
   */
  def getVectorIdAsPerOrd(i: Int): Long = {
    logger.info(s"fetch vector id as per ordinal: ${i}")
    val vectorFrame = vectorsIndex.get(i)
    vectorFrame.mappedBuffer.position(vectorFrame.position.toInt).getLong
  }

  override protected def doStop(): Unit = {
    slots.forEach(mmap => {
      val fileName = mmap.sourceFile
      logger.info(s"close file channel refer to ${fileName} to release mmap when gc.")
      Closables.closeWithLog(mmap.fc)
      // no good way to release mmap in java for the time being, mmap has to be clean up at GC, that's why we clear map to release the reference to them.
    })
    slots.clear()
    vectorsIndex.clear()
  }

  override def size(): Int = vectorsIndex.size()

  override def dimension(): Int = dimension

  /**
   * This is the 1st revision which use simplest way to index vectors, which consumes more memory to hold the necessary information to get vector.
   *
   * A next revision can resort to slots only, by calculating positon at each getVector call.
   *
   * @param i the ordinal to fetch vector with
   * @return the vector data corresponding to the ordinal
   */
  override def getVector(i: Int): VectorFloat[_] = {
    val vectorFrame = vectorsIndex.get(i)
    val mappedByteBuffer = vectorFrame.mappedBuffer
    val position = vectorFrame.position
    val readBytes = new Array[Byte](dimension * java.lang.Float.BYTES) // TODO confirm whether getVector is NOT multithreaded accessed, so that we can resue the read buffer or not?!
    mappedByteBuffer.position(position.toInt + java.lang.Long.BYTES).get(readBytes)
    vts.createFloatVector(ByteAndBuffer.bytesToFloats(readBytes))
  }

  override def isValueShared: Boolean = false

  override def copy(): RandomAccessVectorValues = new MmapSegmentsRandomAccessVectorValues(dataDir, indexDir, collectionName, dimension, autoStart)


}