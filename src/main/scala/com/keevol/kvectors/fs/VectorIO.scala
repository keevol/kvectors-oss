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

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util

/**
 * @deprecated too tricky, resort to a much simpler long + array[float] way.
 */
@Deprecated
trait VectorIO extends AutoCloseable {

  val magicNumber: Array[Byte] = Array(
    0xCE.toByte,
    0xE5.toByte,
    0x07.toByte,
    0xC9.toByte
  )

  val MAGIC_NUMBER_LENGTH = magicNumber.length // 4
  val VERSION_LENGTH = 2 // Short is 2 bytes
  val DIMENSION_LENGTH = 4 // Int is 4 bytes
  val PADDING_LENGTH = 6
  val HEADER_LENGTH_OF_RAW_VECTORS_FILE = MAGIC_NUMBER_LENGTH + VERSION_LENGTH + DIMENSION_LENGTH + PADDING_LENGTH // 4 + 2 + 4 + 6 = 16


  //  val HEADER_LENGTH_OF_RAW_VECTORS_FILE = 16; // 4 bytes of magicNumber + 2 bytes of version + 4 bytes of dimension length + padding

  val padding = Array.ofDim[Byte](PADDING_LENGTH)


  def writeHeaderToBuffer(version: Short, dimension: Int): ByteBuffer = {
    val headerBuffer: ByteBuffer = ByteBuffer.allocate(HEADER_LENGTH_OF_RAW_VECTORS_FILE)
    headerBuffer.put(magicNumber)
    headerBuffer.putShort(version)
    headerBuffer.putInt(dimension)
    headerBuffer.put(padding)
    headerBuffer.flip()
    headerBuffer
  }

  def readHeaderFromBuffer(headerBuffer: ByteBuffer): (Short, Int) = {
    val magicNumberBuffer = new Array[Byte](MAGIC_NUMBER_LENGTH)
    headerBuffer.get(magicNumberBuffer)
    if (!util.Arrays.equals(magicNumber, magicNumberBuffer)) {
      throw new IllegalStateException("NOT kvectors data file!")
    }
    val version = headerBuffer.getShort
    val dimension = headerBuffer.getInt
    // ignore padding read
    (version, dimension)
  }


  def writeHeader(version: Short, dimension: Int, writer: RandomAccessFile): Unit = {
    writer.seek(0)
    writer.write(magicNumber) // 4 bytes
    writer.writeShort(version) // 2 bytes
    writer.writeInt(dimension) // 4 bytes
    writer.write(padding) // padding 6 bytes to align
  }

  /**
   * mainly for validation of data file and retrieve the dimension
   *
   * @param reader io handle
   */
  def readHeader(reader: RandomAccessFile): (Short, Int) = {
    if (reader.length() < HEADER_LENGTH_OF_RAW_VECTORS_FILE) {
      throw new IllegalStateException("broken file?")
    }
    val magicNumberBuffer = new Array[Byte](MAGIC_NUMBER_LENGTH)
    reader.seek(0)
    reader.readFully(magicNumberBuffer) // 不能用read，否则可能导致读取不全。
    if (!util.Arrays.equals(magicNumber, magicNumberBuffer)) {
      throw new IllegalStateException("NOT kvectors data file!")
    }
    val version = reader.readShort()
    val dimension = reader.readInt()
    reader.skipBytes(6) // in case next read is the 1st one

    (version, dimension)
  }

}