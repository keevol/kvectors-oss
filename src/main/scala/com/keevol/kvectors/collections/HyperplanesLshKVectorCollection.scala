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

import com.keevol.kvectors.collections.config.JsonConfig
import com.keevol.kvectors.collections.utils.DropCollectionRoutine
import com.keevol.kvectors.index.lsh.{HyperplanesLSHIndex, HyperplanesLSHIndexBuilder, HyperplanesLSHIndexStore}
import com.keevol.kvectors.repository.LocalFSVectorStore
import com.keevol.kvectors.utils.TypeAlias.Dir
import com.keevol.kvectors.utils.{Closables, KVectorIdGenerator}
import com.keevol.kvectors._
import com.keevol.kvectors.topk.{IdWithScore, CompoundTopKCollector}
import io.vertx.core.json.JsonObject
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.Strings
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory

import java.io.File
import java.util
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
 * NOTE: LSH KVector Collection costs more memory than other Index-typed collection.
 *
 * This type of vector collection is mainly for demonstration, not recommended for production!!!
 *
 *  `search 10000 vectors in 1308161 milli`
 *
 *  The performance is also worse than graph-based index and IVF index.
 *
 *  Furthermore, in order to access index in memory for better performance,
 *  we have to load index states into memory at startup and save at shutdown,
 *  these operations will take a long time to finish which will block the startup and shutdown processes.
 *
 *  So, these 3 points don't make LSH index a good candidate on production. We didn't mention the parameter tuning issues either.
 *
 * @param name vector collection name
 * @param repositoryDir where kvectors' data locates
 * @param k number of hyperplanes in LSH index
 * @param L number of hashtable in LSH index
 * @param dim dimension of vector expected.
 * @param loadAllVectorsIntoMemory whether to load all vector data into memory for fast access, default is true
 */
class HyperplanesLshKVectorCollection(val name: String,
                                      val repositoryDir: Dir,
                                      k: Int,
                                      L: Int,
                                      dim: Int,
                                      loadAllVectorsIntoMemory: Boolean = true,
                                      indexInAsync: Boolean = true,
                                      queryTimeoutInMilli: Long = 50) extends IndexedKVectorsCollection[HyperplanesLSHIndex] with ConfigInspectableVectorCollection {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  private val dataDir = new File(repositoryDir, name) // collection data dir
  if (!dataDir.exists()) {
    FileUtils.forceMkdir(dataDir) // create collection dir if not exists
  }

  private val indexDir = new File(dataDir, "index")
  if (!indexDir.exists()) {
    FileUtils.forceMkdir(indexDir)
  }

  private val vectorStore: LocalFSVectorStore = new LocalFSVectorStore(dataDir, name, loadAllVectorsToMemory = loadAllVectorsIntoMemory)

  /**
   * only one LSH index needed for one vector collection of this type, so just use top index dir as individual index dir
   */
  private val indexStore = new HyperplanesLSHIndexStore(indexDir)

  private val idGenerator = new KVectorIdGenerator(dataDir)

  /**
   * even the index is built successfully, we would not like to enable it right now, so a separate enable op makes thing much clearer.
   *
   * @param indexName the index to be enabled
   */
  override def enableIndex(indexName: String): Unit = logger.info("the LSH will be enabled at startup automatically, no explicit enable needed.")

  /**
   * index build logic in a blocking way which will block caller thread
   * @return index name
   */
  override def buildIndex(): HyperplanesLSHIndex = throw new IllegalCallerException("No explicit index building should be called on a LSH indexed vector collection")


  /**
   * list available index
   *
   * most of the time, for admin ops to select which index to enable.
   *
   * @return available index
   */
  override def listIndexes(): List[String] = List.empty

  /**
   * the index building log
   *
   * @return logs of index building
   */
  override def listIndexBuildingHistory(n: Int): Iterator[String] = Iterator.empty

  override def collectionName(): String = name

  /**
   * load vectors metadata into memory at startup or refresh.
   */
  override def reload(): Unit = {
    try {
      // we need to load or build an LSH index if non-exists
      // this may block startup process for a long time if a lot of vectors in LSH index, but async is ok?!
      // if this really blocks the startup process, the caller of reload should make the call in async!
      val index = indexStore.load()
      activeIndexRegistry.set(index)
    } catch {
      case t: Throwable => {
        logger.warn(s"no LSH index found, we may create one : ${ExceptionUtils.getStackTrace(t)}")
        // we introduce explicit dim parameter in constructor to avoid lazy init at first vector insert.
        // in this way, even empty vector collection, we still can eagerly create hyperplanes of LSH index at startup.
        val indexBuilder = new HyperplanesLSHIndexBuilder(name, k = k, L = L, dimension = dim)
        val lshIndex = indexBuilder.build()
        activeIndexRegistry.set(lshIndex)
      }
    }

    idGenerator.start()
    vectorStore.start()
  }

  /**
   * add vector to collection.
   *
   * @param vector in float32
   */
  override def add(vector: VectorRecord): Unit = {
    require(vector.vector.length == dim, "vector dimension doesn't conform to explicit parameter(dim) defined")

    val id = idGenerator.next()
    vectorStore.add(KVector(id = id, vector = vector.vector, rid = vector.metadata.relationId, meta = vector.metadata.asJson()))

    if (indexInAsync) {
      // index data directly in background
      Future {
        indexVector(id, vector.vector)
      }
    } else {
      indexVector(id, vector.vector)
    }
  }

  private def indexVector(id: Long, vector: Array[Float]): Unit = {
    val idx = activeIndexRegistry.get()
    for (hi <- idx.hashers.indices) {
      val hasher = idx.hashers(hi)
      val lshKey = hasher.hash(VectorFloats.from(vector))
      val hashtable = idx.lsh(hi)
      hashtable.computeIfAbsent(lshKey, _ => new ConcurrentHashMap[Long, Boolean]()).put(id, true)
    }
  }

  /**
   * do similarity search.
   *
   * @param vector        query vector
   * @param topK          result count
   * @param threshold     0.8f as default, higher if you want to make the standard stricter
   * @return a collection of qualified similar vectors or none.
   */
  override def query(vector: Array[Float], topK: Int, threshold: Float): util.List[VectorResult] = {
    val topKCollector = new CompoundTopKCollector(topK, true)
    // query in multiple hyperplanes tables
    val idx = activeIndexRegistry.get()
    if (idx == null) {
      throw new IllegalStateException("no LSH index found or init")
    }
    val futures = (0 until L).map { i =>
      Future {
        logger.debug(s"start ${i} parallel query of ${vector(0)}")
        val hasher = idx.hashers(i)
        val lshKey = hasher.hash(VectorFloats.from(vector))
        val hashtable = idx.lsh(i)
        val iter = hashtable.computeIfAbsent(lshKey, _ => new ConcurrentHashMap[Long, Boolean]()).keySet().iterator()
        while (iter.hasNext) {
          val candidateId = iter.next()
          vectorStore.get(candidateId).foreach(kvec => {
            val score = computeSimilarity(vector, kvec.vector)
            if (score >= threshold) {
              topKCollector.add(IdWithScore(candidateId, score))
            }
          })
        }
        logger.debug(s"end ${i} parallel query of ${vector(0)}")
      }
    }
    logger.debug("wait parallel query return")
    Try(Await.result(Future.sequence(futures), Duration(queryTimeoutInMilli, TimeUnit.MILLISECONDS))) match {
      case Success(_) =>
        logger.info("after topK is ready, wrap and convert to required result type")
        recollectFrom(topKCollector, vectorStore)
      case Failure(t) => throw t
    }
  }

  override def drop(): Unit = DropCollectionRoutine.execute(this, dataDir)

  override def count(): Long = vectorStore.last()

  /**
   * the iterated item can be None, in case it's deleted or missing gap in id sequence.
   */
  override def vectors(): util.Iterator[Option[KVector]] = new util.Iterator[Option[KVector]] {
    private val iterator = vectorStore.scan()

    override def hasNext: Boolean = iterator.hasNext

    override def next(): Option[KVector] = {
      iterator.next().flatMap(lit => vectorStore.get(lit.id))
    }
  }

  override def close(): Unit = {
    val index = activeIndexRegistry.get()
    if (index != null) {
      logger.info("close LSH index store...(it may takes a long time to save index journals)")
      indexStore.save(index)
    }

    Closables.catchAndLog(idGenerator.stop())
    logger.info("close vector store(with metastore) in LSH collection...")
    Closables.catchAndLog(vectorStore.stop())
  }

  override def saveCreationConfig(): Unit = {
    val args = JsonObject.of(
      "name", name,
      "repoDir", repositoryDir.getAbsolutePath,
      "k", k,
      "L", L,
      "dim", dim,
      "loadAllVectorsIntoMemory", loadAllVectorsIntoMemory,
      "indexInAsync", indexInAsync,
      "queryTimeoutInMilli", queryTimeoutInMilli
    )

    saveConfig(args, dataDir)
    //    val cfg = JsonObject.of(
    //      "type", getClass.getName,
    //      "args", args
    //    )
    //
    //    saveConfig(cfg.encode(), JsonConfig.getConfigFileAsPer(dataDir, collectionName()))
  }
}

object HyperplanesLshKVectorCollection extends PartialFunction[Dir, HyperplanesLshKVectorCollection] {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  override def isDefinedAt(collectionDir: Dir): Boolean = {
    val configFile = JsonConfig.getConfigFile(collectionDir)
    if (!configFile.exists()) {
      return false
    }
    val json = JsonConfig.from(configFile)
    if (!json.containsKey("type")) {
      return false
    }

    val expectedClassName = classOf[HyperplanesLshKVectorCollection].getName
    logger.info(s"Expected class name for comparison is: [${expectedClassName}]")

    Strings.CS.equals(json.getString("type"), expectedClassName)
  }

  override def apply(collectionDir: Dir): HyperplanesLshKVectorCollection = {
    val jsonConfig = JsonConfig.from(JsonConfig.getConfigFile(collectionDir)).getJsonObject("args")
    val name = jsonConfig.getString("name")
    val repositoryDir = jsonConfig.getString("repoDir")
    val k = jsonConfig.getInteger("k")
    val L = jsonConfig.getInteger("L")
    val dimension = jsonConfig.getInteger("dim")
    val loadAllVectorsIntoMemory = jsonConfig.getBoolean("loadAllVectorsIntoMemory")
    val indexInAsync = jsonConfig.getBoolean("indexInAsync")
    val queryTimeoutInMilli = jsonConfig.getLong("queryTimeoutInMilli")

    new HyperplanesLshKVectorCollection(
      name = name,
      repositoryDir = new File(repositoryDir),
      k = k,
      L = L,
      dim = dimension,
      loadAllVectorsIntoMemory = loadAllVectorsIntoMemory,
      indexInAsync = indexInAsync,
      queryTimeoutInMilli = queryTimeoutInMilli)
  }
}