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

import com.keevol.kvectors.collections.utils.TopKResultConverter
import com.keevol.kvectors.collections.utils.TopKResultConverter.logger
import com.keevol.kvectors.VectorResult
import com.keevol.kvectors.index.KVectorIndex
import com.keevol.kvectors.repository.{KVectorStore, VecFileList}
import com.keevol.kvectors.topk.CompoundTopKCollector
import io.vertx.core.json.JsonObject
import kong.unirest.Unirest
import org.apache.commons.lang3.time.{DateFormatUtils, DurationFormatUtils}
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference
import scala.beans.BeanProperty

trait IndexedKVectorsCollection[T <: KVectorIndex] extends KVectorCollection with VecFileList {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  protected val activeIndexRegistry: AtomicReference[T] = new AtomicReference[T]() // this is a folder name in fact, since we decide to store index in different subfolders.

  // indexed collection can be on cloud oss, even index store, so file sys attribute is not proper here.
  //  protected val activeIndexSnLFilename = "active.index"

  @BeanProperty
  var maxNumOfIndexArchives: Int = 11 // how many index archives to keep

  /**
   * even the index is built successfully, we would not like to enable it right now, so a separate enable op makes thing much clearer.
   *
   * @param indexName the index to be enabled
   */
  def enableIndex(indexName: String): Unit

  /**
   * index build logic in a blocking way which will block caller thread
   * @return index name
   */
  def buildIndex(): T

  /**
   * index build in background, will return immediately after launching the building job.
   *
   * most of the time, we can resort to [[buildIndex()]] but run it in background thread.
   *
   * @return future of the bg job
   */
  def buildIndexInAsync(enableAfterBuilt: Boolean = true, webhook: Option[String] = None): CompletableFuture[Void] = {
    CompletableFuture.runAsync(() => {
      val start = System.currentTimeMillis()
      val idx = buildIndex()

      if (enableAfterBuilt) {
        logger.info(s"enable index: ${idx.getName} for vector collection: ${collectionName()}")
        enableIndex(idx.getName)
      }
      val end = System.currentTimeMillis()
      val interval = DurationFormatUtils.formatDuration((end - start), "d '天' H '小时' m '分钟' s '秒'")
      val message = s"${DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS")} => 完成 向量集合:${collectionName()} 总共 ${count()} 向量的全量索引构建， 耗时： ${interval}，索引名称：${idx.getName}"
      logger.info(message)

      webhook.foreach(url => {
        val r = Unirest.post(url).charset(StandardCharsets.UTF_8).contentType("application/json").body(JsonObject.of("message", message).encode()).asString()
        if (!r.isSuccess) {
          logger.warn(s"fails to notify webhook at ${url} after index build: status=${r.getStatus}, response body=${r.getBody}")
        }
      })
    })
  }

  /**
   * list available index
   *
   * most of the time, for admin ops to select which index to enable.
   *
   * @return available index
   */
  def listIndexes(): List[String]

  /**
   * the index building log
   *
   * @return logs of index building
   */
  def listIndexBuildingHistory(n: Int = 20): Iterator[String]

  def recollectFrom(topKCollector: CompoundTopKCollector, vectorStore: KVectorStore): java.util.List[VectorResult] = TopKResultConverter.convert(topKCollector, vectorStore)

}