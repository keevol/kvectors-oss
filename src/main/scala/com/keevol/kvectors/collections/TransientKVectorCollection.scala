package com.keevol.kvectors.collections

import com.keevol.kvectors._
import com.keevol.kvectors.topk.{IdWithScore, CompoundTopKCollector}
import io.vertx.core.json.JsonObject
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.util
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, ExecutorService, Executors}
import scala.collection.JavaConverters._

/**
 * 10000条并行计算，还不如串行，串行可以4ms，并行往往13+ms
 *
 * 数据量不够多，并发基础设施反而带来不必要的负担。 最主要，IO和计算的GAP瓶颈不够明显。
 *
 * @param name vector collection's name
 * @param concurrentEnableThreshold enable multi-threading when the count go over this threshold count, mainly for demo, most of the time, we don't need to enable it, since CPU + SIMD is fast enough.
 */
class TransientKVectorCollection(name: String, concurrentEnableThreshold: Int = 200000) extends KVectorCollection {
  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)


  private val store: ConcurrentHashMap[Long, VectorRecord] = new ConcurrentHashMap[Long, VectorRecord]()
  private val executor = new AtomicReference[ExecutorService]()
  private val idGenerator = new AtomicLong()

  override def collectionName(): String = name

  /**
   * load vectors metadata into memory at startup or refresh.
   */
  override def reload(): Unit = {
    logger.info("do nothing at reload, since it's a in-memory store.")
  }

  /**
   * add vector to collection.
   *
   * @param vector in float32
   */
  override def add(vector: VectorRecord): Unit = {
    require(vector != null)
    store.put(idGenerator.getAndIncrement(), vector)
  }

  /**
   * do similarity search.
   *
   * @param qVector        query vector
   * @param topK          result count
   * @param threshold     0.8f as default, higher if you want to make the standard stricter
   * @return a collection of qualified similar vectors or none.
   */
  override def query(qVector: Array[Float], topK: Int, threshold: Float): util.List[VectorResult] = {
    val concurrentEnabled = store.size() > concurrentEnableThreshold
    val topKCollector = if (concurrentEnabled) {
      if (executor.get() == null) {
        executor.compareAndSet(null, Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors()))
      }
      new CompoundTopKCollector(topK, true)
    } else {
      new CompoundTopKCollector(topK)
    }
    if (concurrentEnabled) {
      logger.info("full size of computation is larger than 1thousand, do it in concurrency.")
      val countDownLatch = new CountDownLatch(store.size())
      store.entrySet().forEach(e => {
        val id = e.getKey
        val vr = e.getValue
        executor.get().submit(new Runnable {
          override def run(): Unit = {
            val similarityScore = computeSimilarity(qVector, vr.vector, similarityAlg)
            if (similarityScore >= threshold) {
              topKCollector.add(IdWithScore(id, similarityScore))
            }
            countDownLatch.countDown()
          }
        })
      })
      countDownLatch.await()
    } else {
      logger.info("full size of computation is less than 1thousand, do it in sequence.")
      store.entrySet().forEach(e => {
        val id = e.getKey
        val vr = e.getValue
        val vectorFloat = vr.vector
        val similarityScore = computeSimilarity(qVector, vectorFloat, similarityAlg)
        if (similarityScore >= threshold) {
          topKCollector.add(IdWithScore(id, similarityScore))
        }
      })
    }

    val results = new util.ArrayList[VectorResult]()
    topKCollector.getTopK().asScala.map(e => {
      val id = e.id
      val score = e.score
      val meta = store.get(id).metadata
      VectorResult(e.id, new JsonObject(meta.asJson()), Some(score))
    }).foreach(results.add)
    results
  }

  override def drop(): Unit = {
    logger.info("do nothing at drop, since it's a in-memory store.")
  }

  override def close(): Unit = {
    if (executor.get() != null) {
      executor.get().shutdown()
      executor.set(null)
    }

  }


  override def vectors(): util.Iterator[Option[KVector]] = new util.Iterator[Option[KVector]] {
    private val iter = store.entrySet().iterator()

    override def hasNext: Boolean = iter.hasNext

    override def next(): Option[KVector] = {
      val e = iter.next()
      Option(KVector(e.getKey, e.getValue.vector, e.getValue.metadata.relationId, e.getValue.metadata.asJson()))
    }
  }

  override def count(): Long = idGenerator.get()
}