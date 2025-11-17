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

import com.keevol.kvectors.VectorFloats
import org.apache.commons.math3.ml.clustering.{DoublePoint, KMeansPlusPlusClusterer}

import scala.collection.JavaConverters._

object KMeans {

  /**
   * before calculating the k-means centroids, do [[VectorSampler.reservoirSample()]] first to get a proper amount of vectors as input source.
   *
   * @param vectors the input vectors which usually come from reservoir sampling
   * @param k the number of clusters after k-means clustering, this parameter matters on cluster quality.
   * @param maxIterations max iteration num
   * @return centroids vectors
   */
  def centroids(vectors: List[Array[Float]], k: Int, maxIterations: Int = 100): List[Array[Float]] =
    new KMeansPlusPlusClusterer[DoublePoint](k, maxIterations)
      .cluster(vectors.view.map(v => VectorFloats.toDoublePoint(v)).asJava)
      .asScala
      .map(cc => VectorFloats.fromDoublePoint(cc.getCenter))
      .toList

}