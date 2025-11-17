package com.keevol.kvectors

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

import io.github.jbellis.jvector.vector.{VectorUtil, VectorizationProvider}
import io.github.jbellis.jvector.vector.types.VectorFloat
import org.apache.commons.math3.ml.clustering.{Clusterable, DoublePoint}

import java.util.concurrent.ThreadLocalRandom

/**
 * Util for exchanging data format with JVector.
 */
object VectorFloats {
  // can be globally shared.
  val vts = VectorizationProvider.getInstance().getVectorTypeSupport

  def from(v: Array[Float]): VectorFloat[_] = vts.createFloatVector(v)

  def of(v: Array[Float]): VectorFloat[_] = from(v)

  def randomVector(dim: Int): VectorFloat[_] = {
    val R = ThreadLocalRandom.current()
    val vec = vts.createFloatVector(dim)
    (0 until dim).foreach(i => {
      vec.set(i, R.nextFloat())
      if (R.nextBoolean()) {
        vec.set(i, -vec.get(i))
      }
    })
    VectorUtil.l2normalize(vec)
    vec
  }


  def toDoublePoint(v: Array[Float]): DoublePoint = new DoublePoint(v.map(_.toDouble))

  def fromDoublePoint(dp: Clusterable): Array[Float] = dp.getPoint.map(_.toFloat)


}