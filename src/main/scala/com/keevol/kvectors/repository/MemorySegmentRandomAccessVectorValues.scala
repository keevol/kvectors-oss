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
import com.keevol.kvectors.utils.Closables
import io.github.jbellis.jvector.graph.RandomAccessVectorValues
import io.github.jbellis.jvector.vector.VectorizationProvider
import io.github.jbellis.jvector.vector.types.VectorFloat
import org.slf4j.LoggerFactory

import java.io.File
import java.lang.foreign.{Arena, MemorySegment}
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption.READ
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong

/**
 * a ref handle for easy access
 *
 * @param file the section source
 * @param fc the file channel to file
 * @param arena the shared arena which allocated the memory segment from mmap
 * @param segment the memory segment allocated after mmap
 * @param count vector count in this section
 * @param globalIndexOrd vector index ord in all section segments
 */
case class SectionRef(file: File, fc: FileChannel, arena: Arena, segment: MemorySegment, count: Long, globalIndexOrd: Long)

/**
 * Since both when building and querying index need it, We should not internalize the snapshot logic in it.
 *
 * the snapshot states(including the .vec list) should be frozen outside and pass into MemorySegmentRandomAccessVectorValues.
 *
 * MemorySegmentRandomAccessVectorValues should always take care of frozen read-only data sections.
 *
 * As long as MemorySegmentRandomAccessVectorValues is initialized, it is immutable for read only.
 *
 * @param indexSnapshot the frozen state for random access
 * @param frameLayout vector frame layout definition/spec.
 */
class MemorySegmentRandomAccessVectorValues(indexSnapshot: IndexDataSetSnapshot, frameLayout: VectorFrameLayout) extends RandomAccessVectorValues with AutoCloseable {
  private final val logger = LoggerFactory.getLogger(getClass.getName)
  private final val vts = VectorizationProvider.getInstance().getVectorTypeSupport
  private final val vectorCount = new AtomicLong()
  private final val sectionLadder: ConcurrentSkipListMap[Long, SectionRef] = new ConcurrentSkipListMap[Long, SectionRef]

  import java.lang.{Integer => JInt, Long => JLong}

  // since jvector only cares about the ordinal, we have to maintain a mapping between the ordinal and the real vector id in kvectors
  // Although we save the mappings in a cache, we don't evict them in fact.
  // DON'T think as a memory leak here, we just want a fast access.
  private final val vectorIdOrdinals: Cache[JInt, JLong] = Caffeine.newBuilder().build[JInt, JLong]() // By default, cache instances created by Caffeine will not perform any type of eviction.

  logger.info(s"frame size in memory layout: ${frameLayout.frameSize} with dimension=${frameLayout.dimension}")

  indexSnapshot.getSnapshotSections.foreach(section => {
    val fc = FileChannel.open(section.vecFile.toPath, READ)
    val arena = Arena.ofShared()
    val segment = fc.map(MapMode.READ_ONLY, section.offset, section.size, arena) // will be disposed as long as arena is closed
    val frameCountOf = frameLayout.frameCount(segment)

    val globalIndexOrd = vectorCount.getAndAdd(frameCountOf)
    sectionLadder.put(globalIndexOrd, SectionRef(section.vecFile, fc, arena, segment, frameCountOf, globalIndexOrd))
  })


  override def size(): Int = vectorCount.get().toInt

  override def dimension(): Int = frameLayout.dimension

  /**
   * current impl. will generate a lot of short-live objects, will improve in the future.
   *
   * @param i ordinal of vector
   * @return
   */
  override def getVector(i: Int): VectorFloat[_] = {
    val e = sectionLadder.floorEntry(i)
    if (e == null) {
      throw new IllegalArgumentException("no found")
    }

    val segment = e.getValue.segment
    val globalIndexOrd = e.getValue.globalIndexOrd
    val segmentFrameCount = e.getValue.count
    logger.debug(s"get Vector at ${i} at section: (global offset=${globalIndexOrd} and frame count=${segmentFrameCount})")

    val offset = (i - globalIndexOrd) * frameLayout.frameSize
    logger.debug(s"fetch id and vector at offset: ${offset}")
    val vectorId = frameLayout.getId(segment, offset)
    val vector = frameLayout.getVector(segment, offset)

    vectorIdOrdinals.put(i, vectorId) // for enriching metadata retrieval mapping later on

    logger.debug(s"return vector from ${e.getValue.file} with vector id=${vectorId} at section offset=${offset} and global index ordinal=(i=${i}/ idx=${globalIndexOrd}) which has ${segmentFrameCount} vectors in total.")
    vts.createFloatVector(vector)
  }

  /*
   * after creation, everything is immutable, so concurrent access is ok.
   */
  override def isValueShared: Boolean = false

  /*
   * after creation, everything is immutable, so concurrent access is ok.
   */
  override def copy(): RandomAccessVectorValues = this

  /**
   * since we just call this method after [[getVector]], the vector id should be in there without eviction.
   *
   * @param i the ordinal of vector frame
   * @return vector id in KVectors design
   */
  def getVectorIdAsPerOrd(i: Int): Long = vectorIdOrdinals.getIfPresent(i)

  override def close(): Unit = {
    sectionLadder.values().forEach(ref => {
      Closables.closeWithLog(ref.arena) // close arena to release memory segment related
      Closables.closeWithLog(ref.fc)
    })
    sectionLadder.clear()
  }
}