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
 *        <p>
 *        Copyright 2017 © 杭州福强科技有限公司版权所有 (<a href="https://www.keevol.cn">keevol.cn</a>)
 */

import com.keevol.kvectors.lifecycles.Refreshable
import com.keevol.kvectors.utils.Closables
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.{ByteBuffer, ByteOrder, MappedByteBuffer}
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

case class VectorLite(id: Long, value: Array[Float])

/**
 * each thread, each reader.
 *
 * 不做过度优化，加上id做前缀用于关联向量与metadata
 *
 * @param dataFile
 * @param version
 */
class KRawVectorReader(dataFile: File, version: Short = 1) extends VectorIO with Refreshable {
  private val logger: Logger = LoggerFactory.getLogger(classOf[KRawVectorReader])
  private val VECTOR_ID_LEN = 8 // long type
  private val FLOAT_LANE_LEN = 4 // single float len in vector

  private var vectorReader: RandomAccessFile = _
  private var fc: FileChannel = _
  private var mappedBuffer: MappedByteBuffer = _

  private val dimension: AtomicInteger = new AtomicInteger(0)
  private val idIndexMap = new ConcurrentHashMap[Long, Long]

  private var dataSize: Long = 0
  private var frameSize: Int = 0
  private var count: Long = 0


  override def refresh(): Unit = {
    dimension.set(0)
    idIndexMap.clear()

    if (vectorReader != null) {
      Closables.closeWithLog(vectorReader)
      vectorReader = null
    }
    vectorReader = new RandomAccessFile(dataFile, "r")

    if (fc != null) {
      Closables.closeWithLog(fc)
      fc = null
    }
    fc = vectorReader.getChannel

    if (mappedBuffer != null) {
      mappedBuffer = null
    }

    if (dimension.get() == 0) {
      val (revision, dim) = readHeader(vectorReader)
      require(revision == version)
      dimension.compareAndSet(0, dim)
    } // only read once!

    if (dimension.get() == 0) {
      throw new IllegalStateException(s"invalid dimension metadata: ${dimension.get()}")
    }

    frameSize = VECTOR_ID_LEN + dimension.get() * FLOAT_LANE_LEN
    dataSize = fc.size() - HEADER_LENGTH_OF_RAW_VECTORS_FILE
    count = if (dataSize > 0) dataSize / frameSize else 0

    mappedBuffer = fc.map(FileChannel.MapMode.READ_ONLY, HEADER_LENGTH_OF_RAW_VECTORS_FILE, dataSize)
    mappedBuffer.order(ByteOrder.BIG_ENDIAN)

    (0 until count.toInt).foreach(i => {
      val position = i * frameSize
      val id = mappedBuffer.getLong(position.toInt)
      idIndexMap.put(id, i)
    })
  }


  def getByIndex(index: Int): Option[(Long, Array[Float])] = {
    if (index < 0 || index >= count) {
      None
    } else {
      val position = index * frameSize
      mappedBuffer.position(position)

      val id = mappedBuffer.getLong()

      val vectorBytes = new Array[Byte](dimension.get() * 4)
      mappedBuffer.get(vectorBytes)

      val vector = ByteAndBuffer.bytesToFloats(vectorBytes)
      Some((id, vector))
    }
  }

  def getById(id: Long): Option[Array[Float]] = {
    if (idIndexMap.containsKey(id)) {
      getByIndex(idIndexMap.get(id).toInt).map(_._2)
    } else {
      None
    }
  }

  def allIds(): Array[Long] = {
    val arrayBuffer = new ArrayBuffer[Long]()
    val iter = idIndexMap.keys()
    while (iter.hasMoreElements) {
      arrayBuffer.append(iter.nextElement())
    }
    arrayBuffer.toArray
  }

  /**
   * 通过mmap，每次顺序frame处理就可以了， 原来想通过多个frame一起读然后拆分处理，发现latency反而更长了。
   *
   * 所以，mmap之后，其实io的latency基本已经到天花板了。 批量fetch再切分处理，反而多了一道不必要的循环。
   *
   * @param callback
   */
  def execute(callback: (Long, Array[Float]) => Unit): Unit = {

    (0 until count.toInt).foreach(i => {
      val position = i * frameSize
      val id = mappedBuffer.getLong(position.toInt)
      val vectorBytes = new Array[Byte](dimension.get() * 4)
      mappedBuffer.get(position + VECTOR_ID_LEN, vectorBytes)
      val vector = ByteAndBuffer.bytesToFloats(vectorBytes)

      callback(id, vector)
    })

    //    val step = 1000
    //    val batches = count.toInt / 1000
    //    (0 until batches).foreach(batch => {
    //      val startPosition = frameSize * step * batch
    //      val batchBytes = new Array[Byte](frameSize * step)
    //      mappedBuffer.get(startPosition, batchBytes)
    //      val byteBuffer = ByteBuffer.wrap(batchBytes)
    //      (0 until step).foreach(i => {
    //        val position = frameSize * i
    //        val id = byteBuffer.getLong(position)
    //        val vectorBytes = new Array[Byte](dimension.get() * 4)
    //        byteBuffer.get(position + VECTOR_ID_LEN, vectorBytes)
    //        val vector = ByteAndBuffer.bytesToFloats(vectorBytes)
    //        callback(id, vector)
    //      })
    //    })

  }



  //  override def next(): VectorLite = {
  //    preReadIfNecessary()
  //
  //    if (!hasNext) {
  //      throw new NoSuchElementException()
  //    }
  //
  //
  //    val id = vectorReader.readLong()
  //
  //    val buffer = Array.ofDim[Byte](dimension.get() * 4)
  //    vectorReader.readFully(buffer)
  //    //    (0 until dimension.get()).foreach(_ => buffer.append(vectorReader.readFloat())) // fucking slow 🤪
  //
  //    // 1. 将字节数组包装到 ByteBuffer 中
  //    val byteBuffer = ByteBuffer.wrap(buffer)
  //    // 2. 设置字节序为大端序，必须与写入时保持一致！
  //    byteBuffer.order(ByteOrder.BIG_ENDIAN)
  //    // 3. 将其视为一个浮点数缓冲区
  //    val floatBuffer = byteBuffer.asFloatBuffer()
  //    // 4. 创建一个空的 float 数组来接收结果
  //    val floatArray = new Array[Float](dimension.get())
  //    // 5. 将所有浮点数从缓冲区批量复制到数组中
  //    floatBuffer.get(floatArray)
  //
  //    VectorLite(id, floatArray)
  //  }

  override def close(): Unit = {
    // 对于 MappedByteBuffer 的释放，Java没有直接的unmap方法。
    // 关闭 Channel 是建议的做法，GC最终会回收它。
    mappedBuffer = null

    Closables.closeWithLog(fc)
    Closables.closeWithLog(vectorReader)
  }


}