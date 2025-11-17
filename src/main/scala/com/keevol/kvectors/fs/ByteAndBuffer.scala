package com.keevol.kvectors.fs

import java.nio.{ByteBuffer, ByteOrder}

object ByteAndBuffer {

  def floatsToBytes(floats: Array[Float], byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN): Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(floats.length * 4)
    byteBuffer.order(byteOrder)
    val floatBuffer = byteBuffer.asFloatBuffer()
    floatBuffer.put(floats)
    byteBuffer.array()
  }

  def bytesToFloats(bytes: Array[Byte], byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN): Array[Float] = {
    val floatBuffer = ByteBuffer.wrap(bytes).order(byteOrder).asFloatBuffer()
    val floatArray = new Array[Float](floatBuffer.capacity())
    floatBuffer.get(floatArray)
    floatArray
  }


}