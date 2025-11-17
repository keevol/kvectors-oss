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

import com.keevol.kvectors.collections.utils.DropCollectionRoutine
import com.keevol.kvectors.enums.SimilarityAlg
import com.keevol.kvectors.fs.ByteAndBuffer
import com.keevol.kvectors.repository.{AnnIndexCollectionMetaManager, IndexDataSetSnapshot, MemorySegmentRandomAccessVectorValues, RolloverFileRawVectorRepository, VecFileList, VectorFrameLayout}
import com.keevol.kvectors.topk.{IdWithScore, CompoundTopKCollector}
import com.keevol.kvectors.utils.{Closables, Encoding, IndexDirectoryPurger, KVectorIdGenerator, MapDB, Webhook, With}
import com.keevol.kvectors.{KVector, SearchUnit, VectorFloats, VectorMetadata, VectorRecord, VectorResult}
import io.github.jbellis.jvector.disk.ReaderSupplierFactory
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex
import io.github.jbellis.jvector.graph.similarity.{BuildScoreProvider, DefaultSearchScoreProvider}
import io.github.jbellis.jvector.graph.{GraphIndexBuilder, GraphSearcher}
import io.github.jbellis.jvector.quantization.{PQVectors, ProductQuantization}
import io.github.jbellis.jvector.util.{Bits, PhysicalCoreExecutor}
import io.github.jbellis.jvector.vector.VectorSimilarityFunction
import io.vertx.core.json.{JsonArray, JsonObject}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.FileFilterUtils
import org.apache.commons.lang3.{StringUtils, Strings}
import org.apache.commons.lang3.time.{DateFormatUtils, DurationFormatUtils}
import org.mapdb.{DBMaker, Serializer}
import org.slf4j.LoggerFactory

import java.io.{BufferedOutputStream, DataOutputStream, File, FileFilter}
import java.lang.foreign.Arena
import java.net.URL
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}
import java.util
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, PriorityBlockingQueue}
import scala.collection.JavaConverters._
import java.nio.file.StandardOpenOption._

/**
 * As per investigation a lot, I decided to make this VectorCollection to be memory-for-incremental-vectors and ann-indexed-for-full-vectors implementation.
 *
 * That's, when new vector is added into this vector collection, it will be added to memory store and sync to persistent store asynchronously.
 * We will not index new vector to ANN index, we only full-scan them at query.
 *
 * When at query, we query two places in parallel:
 *
 * 1. the in memory store with full scan;
 * 2. the indexed ANN index if any.
 *
 * At first time, no ANN index will be built since no data exists.
 *
 * As long as we trigger the build and a full-index is built out, we can swap it atomically to serve the query manually or automatically.
 *
 * At next reboot, the full-index will be loaded at startup, so it will continue to serve the ANN query.
 *
 * @param name the collection name
 * @param repositoryDir the kvectors's base dir of data
 * @param maxNumOfIndexArchives to keep how many index archives
 */
class AnnIndexKVectorCollection(val name: String, val repositoryDir: File, maxNumOfIndexArchives: Int = 11) extends KVectorCollection with VecFileList {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  private val INDEX_FILE_SUFFIX = ".idx"
  private val PQVECTORS_FILE_SUFFIX = ".pqv"
  private val INDEX_FILE_NAME = name + INDEX_FILE_SUFFIX
  private val PQV_FILE_NAME = name + PQVECTORS_FILE_SUFFIX

  private val dataDir = new File(repositoryDir, name)
  private val indexDir = new File(dataDir, "index")
  if (!dataDir.exists()) {
    FileUtils.forceMkdir(dataDir)
  }
  if (!indexDir.exists()) {
    FileUtils.forceMkdir(indexDir)
  }

  private val idGenerator = new KVectorIdGenerator(dataDir)
  private val dimensionMetaFile: File = new File(dataDir, name + ".dim")

  private val inMemoryVectorStore: ConcurrentHashMap[Long, VectorRecord] = new ConcurrentHashMap[Long, VectorRecord]()
  private val vectorRepository: RolloverFileRawVectorRepository = new RolloverFileRawVectorRepository(dataDir, name)

  // use mapdb as metadata storage, although we can write to file directly too.
  private val metaFile = new File(dataDir, name + ".mdb")
  private val metaManager: AnnIndexCollectionMetaManager = new AnnIndexCollectionMetaManager(metaFile)

  private val dimensionHolder: AtomicInteger = new AtomicInteger(0)

  /**
   * if an active index is registered, we have to compare .vec file list with its index_vectors_snapshot.lst to exclude the ones that had been indexed
   *
   * and then load the rest of vectors into inMemoryVectorStore.
   */
  private val activeIndexRegistry: AtomicReference[String] = new AtomicReference[String]() // this is a folder name in fact, since we decide to store index in different subfolders.
  private val activeIndexSnLFile = new File(dataDir, "active.index.save.and.load.file")

  /**
   * if enableAfterBuildSuccess is not enabled at full index build, we will keep a reference info here so that we can re-enable the new index later on.
   */
  private val newIndexDirHolder: AtomicReference[File] = new AtomicReference[File]()

  private val searchUnitHolder: AtomicReference[SearchUnit] = new AtomicReference[SearchUnit]()


  override def collectionName(): String = this.name

  /**
   * load vectors metadata into memory at startup or refresh.
   */
  override def reload(): Unit = {

    // reload the id generator
    idGenerator.start()
    //    if (idGeneratorStateFile.exists()) {
    //      idGenerator.set(FileUtils.readFileToString(idGeneratorStateFile, StandardCharsets.UTF_8).toLong)
    //    }
    // reload dimension meta from file
    if (dimensionMetaFile.exists()) {
      val dim = Integer.valueOf(FileUtils.readFileToString(dimensionMetaFile, StandardCharsets.UTF_8))
      if (dim > 0) {
        dimensionHolder.set(dim)
      }
    }
    // load if necessary
    if (activeIndexSnLFile.exists()) {
      logger.info("last active index detected, load and enable it...")
      val activeIndexName = StringUtils.trimToEmpty(FileUtils.readFileToString(activeIndexSnLFile, Encoding.default()))
      activeIndexRegistry.set(activeIndexName)
      enableGraphIndex(new File(indexDir, activeIndexName))
    } else {
      logger.warn("no last active index detected, ignore and continue...")
    }

    val activeIndexName = activeIndexRegistry.get()
    // load vectors that are not indexed by ANN before writer(the repository) starts
    val snapshotSections = if (StringUtils.isNotEmpty(activeIndexName)) {
      val snapshot = new IndexDataSetSnapshot(new File(indexDir, activeIndexName))
      snapshot.getSnapshotSections
    } else {
      List()
    }
    val vecFiles = listVecFilesWithOrder(dataDir, collectionName())
    val frameLayout = new VectorFrameLayout(dimensionHolder.get())
    (0 until vecFiles.size()).foreach(i => {
      val vecFileName = vecFiles.get(i)
      val vecFile = new File(dataDir, vecFileName)
      val interleaveFileOption = snapshotSections.find(section => Strings.CS.equals(section.vecFile.getName, vecFileName))
      if (interleaveFileOption.isDefined) {
        // check whether they are interleaved furthermore
        val indexedFile = interleaveFileOption.get
        if (indexedFile.size < vecFile.length()) {
          collectVectorFrom(vecFile, indexedFile.size, frameLayout) // collect the left vectors which are not indexed by ANN
        }
      } else {
        collectVectorFrom(vecFile, 0, frameLayout) // load all vectors in the file
      }
    })

    // start the backend repo to accept new vector to persist after old vectors are loaded into in-memory store
    vectorRepository.start()
  }

  // load each vector file that is not indexed via ANN(including the partial one)
  private def collectVectorFrom(vecFile: File, start: Long, frameLayout: VectorFrameLayout): Unit = {
    val fc = FileChannel.open(vecFile.toPath, StandardOpenOption.READ)
    With(fc) {
      val arena = Arena.ofConfined()
      With(arena) {
        val segment = fc.map(MapMode.READ_ONLY, start, vecFile.length(), arena)
        val count = frameLayout.frameCount(segment)
        (0 until count.toInt).foreach(i => {
          val offset = i * frameLayout.frameSize
          val id = frameLayout.getId(segment, offset)
          val vector = frameLayout.getVector(segment, offset)
          val metaOption = metaManager.get(id)
          val meta = if (metaOption.isDefined) {
            VectorMetadata.fromJsonString(metaManager.get(id).get.encode())
          } else {
            throw new IllegalStateException("THIS SHOULD NOT HAPPEN, at least rid should be there!")
          }

          inMemoryVectorStore.put(id, VectorRecord(vector = vector, metadata = meta))
        })
      }
    }
  }

  /**
   * add vector to collection.
   *
   * In this impl. we only add vector to in memory store and sync to file system without any realtime indexing.
   *
   * we will merge query results from both this in-memory store and vectors from ANN index and RAVV.
   *
   * At startup, we also need to load intersect vectors into in-memory store which are not in ANN index.
   *
   * @param vector in float32
   */
  override def add(vector: VectorRecord): Unit = {
    // 应该先放内存map，再异步序列化到FS
    // 索引的构建可以只在后台异步执行，跟向量添加就没啥关系了
    // 查询的时候，合并内存与ANN索引查询的结果并去重就可以了！
    if (dimensionHolder.get() == 0) {
      // dimension can't be preset at creation of vector collection, so we have to capture it at first insert.
      // Milvus will set dimension at collection creation, we allow dynamic dimension in KVectors.
      logger.info("first vector comes in, set its dimension as following standard.")
      dimensionHolder.set(vector.vector.length)
      FileUtils.writeStringToFile(dimensionMetaFile, String.valueOf(dimensionHolder.get()), StandardCharsets.UTF_8, false)
    }

    require(vector.vector.length == dimensionHolder.get(), s"vector dimension collision, expected:${dimensionHolder.get()}, get: ${vector.vector.length}")
    require(vector.metadata != null, "metadata is a must to connect vector to its source of truth.")

    val id = idGenerator.next()
    // 1. save to memory store
    inMemoryVectorStore.put(id, vector)
    // 2. save vector and its metadata to different backend files
    vectorRepository.append(id, vector.vector)
    metaManager.add(id, new JsonObject(vector.metadata.asJson()))
  }

  private def similarityFunction: VectorSimilarityFunction = if (similarityAlg == SimilarityAlg.DOT_PRODUCT) VectorSimilarityFunction.DOT_PRODUCT else VectorSimilarityFunction.COSINE

  def getIndexBuildHistory: Array[String] = metaManager.getIndexBuildHistory


  /**
   * This will create ANN index in background which will take a long-long time
   *
   * most of the time, this will be performed by ops in front. (OR regularly via scheduler?)
   *
   * After it's done, similarity search can be performed on this collection.
   *
   * We don't do incremental indexing, we just do full-index + in-memory full-scan search in this impl.
   *
   * @return async notification if webhook is configured.
   */
  def buildFullIndexAsync(enableAfterBuildSuccess: Boolean = true, webhook: Option[URL] = None): CompletableFuture[Void] = {
    require(dimensionHolder.get() > 0)

    logger.info("full index build request accepted.")
    val start = System.currentTimeMillis()
    metaManager.addLog(start, s"start new full index build for ${name} at ${DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS")}")

    CompletableFuture.runAsync(() => {
      val newIndexDir = new File(indexDir, start.toString)
      FileUtils.forceMkdir(newIndexDir)
      newIndexDirHolder.set(newIndexDir) // mark the latest index for auto-enable at end
      logger.info(s"build index: $newIndexDir")

      val indexFile = new File(newIndexDir, INDEX_FILE_NAME)
      val pqVectorsFile: File = new File(newIndexDir, PQV_FILE_NAME)

      val frameLayout = new VectorFrameLayout(dimensionHolder.get())
      val snapshot = new IndexDataSetSnapshot(newIndexDir)
      snapshot.snapshot(dataDir, collectionName(), frameLayout)

      val ravv = new MemorySegmentRandomAccessVectorValues(snapshot, frameLayout)
      try {
        // compress vectors to make build fast and use less memory
        val pq: ProductQuantization = ProductQuantization.compute(ravv, 16, 256, !(similarityAlg == SimilarityAlg.DOT_PRODUCT));
        val pqv: PQVectors = pq.encodeAll(ravv, PhysicalCoreExecutor.pool())
        val bsp = BuildScoreProvider.pqBuildScoreProvider(similarityFunction, pqv);
        //        val bsp = BuildScoreProvider.randomAccessScoreProvider(ravv, similarityFunction)
        val graphIndexBuilder = new GraphIndexBuilder(bsp, dimensionHolder.get(), 40, 60, 1.2f, 1.2f, true)
        val graphIndex = graphIndexBuilder.build(ravv)
        graphIndexBuilder.cleanup()

        logger.debug(s"Built index size: ${graphIndex.size(0)}")
        logger.debug(s"Built index max level: ${graphIndex.getMaxLevel}")

        //   ---     DON'T use save !!! it's only for testing purpose, not production. ---
        //        val indexOutput = getDataOutputTo(indexFile)
        //        With(indexOutput) {
        //          graphIndex.save(indexOutput)
        //        }
        OnDiskGraphIndex.write(graphIndex, ravv, indexFile.toPath)

        // FUCK! 还是得存pqv，否则搞不到SearchScoreProvider，搞不到SearchScoreProvider，那就没法调用有score threshold的search方法！！！
        val pqvOutput = getDataOutputTo(pqVectorsFile)
        With(pqvOutput) {
          pqv.write(pqvOutput)
        }

        val end = System.currentTimeMillis()
        val interval = DurationFormatUtils.formatDuration((end - start), "d '天' H '小时' m '分钟' s '秒'")
        val message = s"${DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS")} => 完成 向量集合:${name} 总共 ${ravv.size()} 向量的全量索引构建， 耗时： ${interval}，索引名称：${newIndexDir.getName}(location=${newIndexDir.getAbsolutePath})"
        logger.info(message)
        metaManager.addLog(start, message)

        // notify if needed.
        webhook.foreach(url => {
          Webhook.notify(url, JsonObject.of("total", ravv.size(), "duration", (end - start), "message", message))
        })

        if (enableAfterBuildSuccess) {
          logger.info(s"enable new graph index after build: ${newIndexDir}")

          enableLastestGraphIndex()
        }
      } finally {
        ravv.close()
      }

      logger.info(s"purge legacy index if necessary (to keep max num of ${maxNumOfIndexArchives})")
      IndexDirectoryPurger.purge(indexDir, maxNumOfIndexArchives)
    })
  }

  /**
   * swap latest fully-build graph index to the working one.
   */
  def enableLastestGraphIndex(): Unit = {
    // load and swap
    val idxDir = newIndexDirHolder.get()
    if (idxDir == null) {
      throw new IllegalStateException("no latest index found.")
    }

    val ts = System.currentTimeMillis()
    metaManager.addLog(ts, s"index: ${idxDir.getName} is enabled online at ${DateFormatUtils.format(ts, "yyyy-MM-dd HH:mm:ss.SSS")}")

    enableGraphIndex(idxDir)
  }

  /**
   * for web console API.
   *
   * @param indexName subfolder name of index
   */
  def enableGraphIndex(indexName: String): Unit = {
    val idxDir = new File(indexDir, indexName)
    if (!idxDir.exists()) {
      throw new IllegalArgumentException("no such index exists.")
    }

    val ts = System.currentTimeMillis()
    metaManager.addLog(ts, s"index: ${idxDir.getName} is enabled online at ${DateFormatUtils.format(ts, "yyyy-MM-dd HH:mm:ss.SSS")}")

    enableGraphIndex(idxDir)
  }

  /**
   * open list so that we can put a select element on web console for user to select from.
   *
   * Only index subfolder names are returned, no absolute path name exposed.
   *
   * @return subfolder name of index
   */
  def listIndex(): JsonArray = {
    FileUtils.listFilesAndDirs(indexDir, null, FileFilterUtils.directoryFileFilter()).asScala
      .map(_.getName)
      .map(name => JsonObject.of("name", name, "displayName", DateFormatUtils.format(name.toLong, "yyyy-MM-dd HH:mm:ss.SSS")))
      .foldLeft(new JsonArray())((a, json) => a.add(json))
  }

  private def enableGraphIndex(indexDir: File): Unit = {
    val activeIndexName = indexDir.getName
    logger.info(s"try to swap graph index to ${activeIndexName}")

    val readerSupplier = ReaderSupplierFactory.open(new File(indexDir, INDEX_FILE_NAME).toPath)

    val onDiskGraphIndex = OnDiskGraphIndex.load(readerSupplier)
    val pqvReader = ReaderSupplierFactory.open(new File(indexDir, PQV_FILE_NAME).toPath)

    val pqv = PQVectors.load(pqvReader.get())
    val snapshot = new IndexDataSetSnapshot(indexDir) // at read side, we don't do snapshot, just let it load at initialization
    val vectorFrameLayout = new VectorFrameLayout(dimensionHolder.get())
    val ravv = new MemorySegmentRandomAccessVectorValues(snapshot, vectorFrameLayout)
    val searchUnit = SearchUnit(pqv, onDiskGraphIndex, new GraphSearcher(onDiskGraphIndex), ravv, pqvReader, readerSupplier)
    val oldSearchUnit = searchUnitHolder.get()
    logger.info(s"oldSearchUnit : ${oldSearchUnit}")
    try {
      searchUnitHolder.set(searchUnit)

      logger.info(s"persist mark for new active index: ${activeIndexName}")
      FileUtils.writeStringToFile(activeIndexSnLFile, activeIndexName, Encoding.default(), false)

      logger.info(s"mark new active index to: ${activeIndexName}")
      activeIndexRegistry.set(activeIndexName)

      logger.info(s"new active index: ${activeIndexName} is online NOW!")

      logger.info("evict in-memory redundant vectors...(may cause CPU spike?!)")
      val tmpCounter = new AtomicLong()
      while (tmpCounter.get() < snapshot.getLastVectorId) {
        val vid = tmpCounter.getAndIncrement()
        if (inMemoryVectorStore.containsKey(vid)) {
          inMemoryVectorStore.remove(vid)
        }
      }
    } finally {
      logger.info("clean up old graph index search resources if any...")
      cleanUpSearchUnitResources(oldSearchUnit)
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
    //    val topKCollectorForFullScan = new TopKCollector(topK)
    //    val topKCollectorForAnn = new TopKCollector(topK)
    val topKCollector = new CompoundTopKCollector(topK, true)
    val f1 = CompletableFuture.runAsync(() => {
      inMemoryVectorStore.forEach((id, v) => {
        val score = computeSimilarity(vector, v.vector)
        if (score >= threshold) {
          topKCollector.add(IdWithScore(id, score))
        }
      })
    })
    val f2 = CompletableFuture.runAsync(() => {
      val unit = searchUnitHolder.get()
      if (unit != null) {
        val searcher = unit.searcher
        val index: OnDiskGraphIndex = unit.index

        val qv = VectorFloats.from(vector)
        val asf = unit.pqv.precomputedScoreFunctionFor(qv, similarityFunction)
        val sf = index.getView.rerankerFor(qv, similarityFunction)

        val searchResult = searcher.search(new DefaultSearchScoreProvider(asf, sf), topK, threshold, Bits.ALL)
        searchResult.getNodes.foreach(nodeScore => {
          val id = unit.ravv.asInstanceOf[MemorySegmentRandomAccessVectorValues].getVectorIdAsPerOrd(nodeScore.node)
          topKCollector.add(IdWithScore(id, nodeScore.score))
        })
      } else {
        logger.info("no searcher unit found, no ANN index search involved.")
      }
    })
    CompletableFuture.allOf(f1, f2).join()

    val results = new util.ArrayList[VectorResult]()
    topKCollector.getTopK().asScala.map(e => {
      val meta = metaManager.get(e.id).getOrElse(new JsonObject())
      VectorResult(e.id, meta, Some(e.score))
    }).foreach(results.add)
    results
  }

  override def close(): Unit = {
    // save last id
    Closables.catchAndLog(idGenerator.stop())
    //    FileUtils.writeStringToFile(idGeneratorStateFile, idGenerator.get().toString, StandardCharsets.UTF_8, false)

    cleanUpSearchUnitResources(searchUnitHolder.get())

    // stop the async persistence task gracefully.
    Closables.closeWithLog(vectorRepository)

    Closables.closeWithLog(metaManager)
  }

  private def cleanUpSearchUnitResources(searchUnit: SearchUnit): Unit = {
    if (searchUnit != null) {
      logger.info("clean up OLD graph index search unit resources ...")
      Closables.closeWithLog(searchUnit.searcher)
      Closables.closeWithLog(searchUnit.index)
      Closables.closeWithLog(searchUnit.pqvReaderSupplier)
      Closables.closeWithLog(searchUnit.indexReaderSupplier)
      Closables.closeWithLog(searchUnit.ravv.asInstanceOf[MemorySegmentRandomAccessVectorValues])
    } else {
      logger.info("no old search unit, ignore cleanup")
    }
  }

  private def getDataOutputTo(target: File): DataOutputStream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(target.toPath)))

  override def vectors(): util.Iterator[Option[KVector]] = throw new UnsupportedOperationException("not implemented yet.")

  override def count(): Long = idGenerator.current()

  override def drop(): Unit = DropCollectionRoutine.execute(this, dataDir)
}