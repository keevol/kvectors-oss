package com.keevol.kvectors.topk

import com.keevol.kvectors.{KVectorLite, VectorFloats}
import io.github.jbellis.jvector.vector.VectorUtil

/**
 * Utility class
 */
object TopK {

  def collect(qv: Array[Float], kvec: KVectorLite, topKCollector: TopKCollector[IdWithScore], threshold: Float): Unit = {
    val score = VectorUtil.cosine(VectorFloats.from(qv), VectorFloats.from(kvec.value))
    if (score >= threshold) {
      topKCollector.add(IdWithScore(kvec.id, score))
    }
  }

  def collect(qv: Array[Float], kvec: KVectorLite, topKCollector: CompoundTopKCollector, threshold: Float): Unit = {
    val score = VectorUtil.cosine(VectorFloats.from(qv), VectorFloats.from(kvec.value))
    if (score >= threshold) {
      topKCollector.add(IdWithScore(kvec.id, score))
    }
  }
}

