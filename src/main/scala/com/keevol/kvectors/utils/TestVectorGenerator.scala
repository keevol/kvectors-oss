package com.keevol.kvectors.utils

import java.util.Random
import scala.collection.mutable.ArrayBuffer

object TestVectorGenerator {
  private val rnd = new Random()

  def generateVectorOf(dimension: Int = 768): Array[Float] = {
    val buffer = new ArrayBuffer[Float]()
    (0 until dimension).foreach(_ => buffer.append(rnd.nextFloat()))
    buffer.toArray
  }

}