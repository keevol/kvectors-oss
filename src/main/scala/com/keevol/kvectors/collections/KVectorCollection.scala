package com.keevol.kvectors.collections

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

import com.keevol.kvectors.enums.{CompressionStrategy, IndexStrategy, SimilarityAlg}
import com.keevol.kvectors.{KVector, VectorFloats, VectorRecord, VectorResult}
import io.github.jbellis.jvector.vector.VectorUtil

import scala.beans.BeanProperty

trait KVectorCollection extends AutoCloseable {

  @BeanProperty
  var indexStrategy: IndexStrategy = IndexStrategy.NO_INDEX; // brutal force full scan to do similarity compare, usually, vector count less than 10 thousand.

  @BeanProperty
  var compressionStrategy: CompressionStrategy = CompressionStrategy.NO; // most of the time, we don't compress any vector since we store vectors in row instead of in col as block/chunk.

  @BeanProperty
  var similarityAlg: SimilarityAlg = SimilarityAlg.COSINE // building index and querying with index both need a same similarity alg!!!

  def collectionName(): String

  /**
   * load vectors metadata into memory at startup or refresh.
   */
  def reload(): Unit

  /**
   * add vector to collection.
   *
   * @param vector in float32
   */
  def add(vector: VectorRecord): Unit

  /**
   * do similarity search.
   *
   * @param vector        query vector
   * @param topK          result count
   * @param threshold     0.8f as default, higher if you want to make the standard stricter
   * @return a collection of qualified similar vectors or none.
   */
  def query(vector: Array[Float], topK: Int = 11, threshold: Float = 0.8f): java.util.List[VectorResult]

  def drop(): Unit


  def count(): Long

  /**
   * the iterated item can be None, in case it's deleted or missing gap in id sequence.
   */
  def vectors(): java.util.Iterator[Option[KVector]]


  def computeSimilarity(f1: Array[Float], f2: Array[Float], similarityAlg: SimilarityAlg = SimilarityAlg.COSINE): Float = {
    val similarityScore = similarityAlg match {
      case SimilarityAlg.COSINE => VectorUtil.cosine(VectorFloats.from(f1), VectorFloats.from(f2))
      case SimilarityAlg.DOT_PRODUCT => VectorUtil.dotProduct(VectorFloats.from(f1), VectorFloats.from(f2))
      case SimilarityAlg.EUCLIDEAN => throw new UnsupportedOperationException("most of the time, you will never encounter such situation.")
    }
    similarityScore
  }

}