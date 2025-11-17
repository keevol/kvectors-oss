package com.keevol.kvectors.collections

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

import com.keevol.kvectors.collections.utils.DropCollectionRoutine
import com.keevol.kvectors.enums.SimilarityAlg
import com.keevol.kvectors.topk.{IdWithScore, CompoundTopKCollector}
import com.keevol.kvectors.utils.{Closables, KVectorIdGenerator, MapDB}
import com.keevol.kvectors.{KVector, VectorFloats, VectorMetadata, VectorRecord, VectorResult}
import io.github.jbellis.jvector.vector.VectorUtil
import io.vertx.core.json.JsonObject
import org.apache.commons.io.FileUtils
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap
import org.mapdb.{DBMaker, Serializer}
import org.slf4j.LoggerFactory

import java.io.File
import java.util
import scala.collection.JavaConverters._

/**
 * this impl. is not aimed for huge collections of vector.
 *
 * single node with less than 10 to 1000 thousand vectors is preferred.
 *
 * To get proper latency when more vectors are added into this collection, enable allInMemory option is preferred, although it will cost more memory too.
 */
class MapdbKVectorCollection(val name: String, val repositoryDir: File, allInMemory: Boolean = true) extends KVectorCollection {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  private val dataDir = new File(repositoryDir, name)
  if (!dataDir.exists()) {
    FileUtils.forceMkdir(dataDir)
  }
  private val idGenerator = new KVectorIdGenerator(dataDir)
  // if transactionEnable, commit is a must at each put
  private val mapDB = MapDB(new File(dataDir, name + ".mdb"))
  private val vectorStore = mapDB.hashMap("vectors.store", Serializer.LONG, Serializer.FLOAT_ARRAY).createOrOpen()
  private val metaStore = mapDB.hashMap("meta.store", Serializer.LONG, Serializer.STRING).createOrOpen()
  //  private val idGeneratorStore = mapDB.atomicLong("id.generator").createOrOpen()

  // only filled when allInMemory is enabled.
  private val inMemoryVectorStore: ConcurrentHashMap[Long, VectorRecord] = new ConcurrentHashMap[Long, VectorRecord]()

  override def collectionName(): String = name

  /**
   * load vectors metadata into memory at startup or refresh.
   */
  override def reload(): Unit = {
    idGenerator.start()
    // when all in memory requested, load both vectors and meta into memory at bootstrap
    if (allInMemory) {
      logger.info("load all data into memory at (re)load...")
      val iter = vectorStore.entrySet().iterator()
      while (iter.hasNext) {
        val e = iter.next()
        val id = e.getKey
        val vector = e.getValue
        val metadata = VectorMetadata.fromJsonString(metaStore.get(id))
        inMemoryVectorStore.put(id, VectorRecord(vector, metadata))
      }
    }
  }

  /**
   * add vector to collection.
   *
   * @param vector in float32
   */
  override def add(vector: VectorRecord): Unit = {
    val id = idGenerator.next()
    // put into memory if required.
    if (allInMemory) {
      inMemoryVectorStore.put(id, vector)
    }
    // always persist to disk
    vectorStore.put(id, vector.vector)
    val meta = vector.metadata
    metaStore.put(id, meta.asJson())
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
    val topKCollector = new CompoundTopKCollector(topK)

    val computationFunc = (id: Long, vectorFloat: Array[Float]) => {
      val similarityScore = computeSimilarity(qVector, vectorFloat, similarityAlg)
      if (similarityScore >= threshold) {
        topKCollector.add(IdWithScore(id, similarityScore))
      }
    }

    if (allInMemory) {
      inMemoryVectorStore.entrySet().forEach(e => computationFunc(e.getKey, e.getValue.vector))
    } else {
      vectorStore.entrySet().forEach(e => computationFunc(e.getKey, e.getValue))
    }

    val results = new util.ArrayList[VectorResult]()
    topKCollector.getTopK().asScala.map(e => {
      val id = e.id
      val score = e.score
      val metadata = if (allInMemory) inMemoryVectorStore.get(id).metadata.asJson() else metaStore.get(id)
      VectorResult(e.id, new JsonObject(metadata), Some(score))
    }).foreach(results.add)
    results
  }

  override def vectors(): util.Iterator[Option[KVector]] = new util.Iterator[Option[KVector]] {
    private val iter = vectorStore.entrySet().iterator()

    override def hasNext: Boolean = iter.hasNext

    override def next(): Option[KVector] = {
      val e = iter.next()
      val id = e.getKey
      val vector = e.getValue
      val meta = VectorMetadata.fromJsonString(metaStore.get(id))
      Option(KVector(id, vector, meta.relationId, meta.asJson()))
    }
  }

  override def close(): Unit = {
    Closables.catchAndLog(idGenerator.stop())
    mapDB.close()
  }

  override def count(): Long = idGenerator.current()

  override def drop(): Unit = DropCollectionRoutine.execute(this, dataDir)
}