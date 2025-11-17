package com.keevol.kvectors.index.lsh

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

import io.github.jbellis.jvector.vector.VectorUtil
import io.github.jbellis.jvector.vector.types.VectorFloat

import java.math.BigInteger

trait LshHasher {
  def getName: String

  def hash(v: VectorFloat[_], normalized: Boolean = true): LSHKey
}

class HyperplanesHasher(val name: String, val initialHyperplanes: Array[VectorFloat[_]]) extends LshHasher {
  override def hash(v: VectorFloat[_], normalized: Boolean = true): LSHKey = {
    initialHyperplanes.length match {
      case 8 =>
        ByteLSHKey(calculateKeycode(v).byteValue())
      case 16 =>
        ShortLSHKey(calculateKeycode(v).shortValue())
      case 32 =>
        IntLSHKey(calculateKeycode(v).intValue())
      case 64 =>
        LongLSHKey(calculateKeycode(v).longValue())
      case _ =>
        BigIntLSHKey(calculateKeycode(v))
    }
  }

  private def calculateKeycode(v: VectorFloat[_], normalized: Boolean = true): BigInteger = {
    initialHyperplanes.zipWithIndex.foldLeft(BigInteger.ZERO) {
      case (currentCode, (hyperplane, i)) =>
        val score = if (normalized) {
          VectorUtil.dotProduct(v, hyperplane)
        } else {
          VectorUtil.cosine(v, hyperplane)
        }

        if (score > 0) {
          currentCode.setBit(i)
        } else {
          currentCode
        }
    }
  }

  override def getName: String = name
}
