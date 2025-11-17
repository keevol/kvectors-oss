package com.keevol.kvectors.utils

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

import java.util.concurrent.ThreadLocalRandom

object RandomVector {

  /**
   * generate gaussian random number backed random vector, mainly for LSH hyperplane.
   *
   * @param dimension the dimension of vector
   * @param normalize 在 SimHash (`h(v) = sign(r · v)`) 這種只關心點積**符號**的 LSH 實現中，歸一化並**不是必需的**，因為向量的長度不影響點積的正負號。但在其他一些需要用到點積具體值的 LSH 變種中，這一步就很重要。
   * @return
   */
  def apply(dimension: Int, normalize: Boolean = true): Array[Float] = {
    val buf = new Array[Float](dimension)
    var norm = 0.0

    for (i <- buf.indices) {
      val value = ThreadLocalRandom.current().nextGaussian().toFloat
      buf.update(i, value)
      if (normalize) {
        norm += value * value
      }
    }
    if (normalize) {
      val magnitude = Math.sqrt(norm).toFloat
      if (magnitude > 0) {
        for (i <- buf.indices) {
          buf.update(i, buf(i) / magnitude)
        }
      }
    }
    buf
  }

}