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

import java.lang.foreign.MemoryLayout.PathElement
import java.lang.foreign.{MemoryLayout, MemorySegment, ValueLayout}
import java.nio.ByteOrder


class VectorFrameLayout(val dimension: Int) {

  // --- 1. 定义常量 ---

  // 定义基础数据类型布局，并指定字节序（对于文件IO，明确指定很重要）
  private val ID_LAYOUT = ValueLayout.JAVA_LONG.withOrder(ByteOrder.BIG_ENDIAN)
  private val VECTOR_ELEMENT_LAYOUT = ValueLayout.JAVA_FLOAT.withOrder(ByteOrder.BIG_ENDIAN)

  // 手动计算每个字段的大小
  private val ID_SIZE_BYTES = ID_LAYOUT.byteSize()
  private val VECTOR_SIZE_BYTES = dimension * VECTOR_ELEMENT_LAYOUT.byteSize()

  // 手动计算每个字段在帧内的起始偏移量 (紧凑布局)
  private val ID_OFFSET_IN_FRAME = 0L
  private val VECTOR_OFFSET_IN_FRAME = ID_OFFSET_IN_FRAME + ID_SIZE_BYTES // id 后面紧跟着 vector

  // 计算总的、无填充的帧大小
  private val FRAME_SIZE_BYTES = ID_SIZE_BYTES + VECTOR_SIZE_BYTES

  // --- 2. 提供公开的访问方法 ---

  /**
   * 返回精确的、无填充的帧大小。
   */
  def frameSize: Long = FRAME_SIZE_BYTES

  /**
   * 根据 Segment 总大小计算包含多少帧。
   */
  def frameCount(segment: MemorySegment): Long = segment.byteSize() / frameSize

  /**
   * 从给定的帧偏移量处读取 ID。
   *
   * @param segment     持有所有数据的 MemorySegment
   * @param frameOffset 目标帧的起始字节偏移量
   * @return vector id
   */
  def getId(segment: MemorySegment, frameOffset: Long): Long = {
    // 直接使用 get 方法，并传入精确的绝对偏移量
    segment.get(ID_LAYOUT, frameOffset + ID_OFFSET_IN_FRAME)
  }

  /**
   * 从给定的帧偏移量处读取整个向量。
   *
   * @param segment     持有所有数据的 MemorySegment
   * @param frameOffset 目标帧的起始字节偏移量
   * @return 向量的 float 数组
   */
  def getVector(segment: MemorySegment, frameOffset: Long): Array[Float] = {
    // 计算向量数据的绝对起始偏移量
    val vectorAbsoluteOffset = frameOffset + VECTOR_OFFSET_IN_FRAME

    // 从该偏移量开始，切分出向量所需大小的内存片
    val vectorSlice = segment.asSlice(vectorAbsoluteOffset, VECTOR_SIZE_BYTES)

    // 将内存片转换为 float 数组
    vectorSlice.toArray(VECTOR_ELEMENT_LAYOUT)
  }

  def updateVector(segment: MemorySegment, frameOffset: Long, newValue: Array[Float]): Unit = {
    require(newValue.length == dimension)

    val vectorAbsoluteOffset = frameOffset + VECTOR_OFFSET_IN_FRAME
    val vectorSlice = segment.asSlice(vectorAbsoluteOffset, VECTOR_SIZE_BYTES)

    vectorSlice.copyFrom(MemorySegment.ofArray(newValue))
  }
}


/**
 * Spec. to read vector frames mapped from file with MemorySegment.
 *
 * @param dimension dimension of vector
 */
class VectorFrameLayoutV1(val dimension: Int) {
  private val idName = "id"
  private val vectorName = "vector"

  private val vectorLayout = MemoryLayout.sequenceLayout(dimension, ValueLayout.JAVA_FLOAT.withOrder(ByteOrder.BIG_ENDIAN)).withName(vectorName) // <<--byte order issue if no explicit assign

  private val layout = MemoryLayout.structLayout(
    ValueLayout.JAVA_LONG.withName(idName).withOrder(ByteOrder.BIG_ENDIAN), // <<--byte order issue if no explicit assign
    vectorLayout
  )

  private val idVar = layout.varHandle(PathElement.groupElement(idName))
  private val vectorVar = layout.varHandle(
    PathElement.groupElement(vectorName), // 首先进入名为 "vector" 的组
    PathElement.sequenceElement() // PathElement.sequenceElement() 用于选择序列（数组）中的元素。调用 varHandle 时不指定索引，意味着这个句柄可以访问序列中的 任意 元素，索引将在调用 get/set 时提供。
  )
  private val vectorOffsetInFrame = layout.byteOffset(PathElement.groupElement(vectorName))

  def frameSize: Long = layout.byteSize()

  def frameCount(segment: MemorySegment): Long = segment.byteSize() / frameSize

  /**
   * segment and frame offset should match:
   *
   * if use sliced segment, then offset can be in-frame one;
   * if use global (whole) segment, then offset should be the global one.
   *
   * @param segment data holder
   * @param frameOffset offset in segment to locate id
   * @return vector id
   */
  def getId(segment: MemorySegment, frameOffset: Long): Long = idVar.get(segment, frameOffset)

  def getVector(segment: MemorySegment, frameOffset: Long): Array[Float] = {
    val vectorAbsoluteOffset = frameOffset + vectorOffsetInFrame
    val vectorSlice = segment.asSlice(vectorAbsoluteOffset, vectorLayout.byteSize())
    vectorSlice.toArray(ValueLayout.JAVA_FLOAT)
  }

}