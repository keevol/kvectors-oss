package com.keevol.kvectors.collections.utils

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

import com.keevol.kvectors.repository.KVectorStore
import com.keevol.kvectors.VectorResult
import com.keevol.kvectors.topk.CompoundTopKCollector
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

object TopKResultConverter {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * make a single source of truth with best practice of scala FP
   *
   * FROM:
   *     //    topKCollector.getTopK().asScala.map(idWithScore => {
   *     //      val vecOpt = vectorStore.get(idWithScore.id)
   *     //      require(vecOpt.isDefined)
   *     //      val kvector = vecOpt.get
   *     //      VectorResult(kvector.id, JsonObject.of("rid", kvector.rid, "meta", kvector.meta), score = Some(idWithScore.score))
   *     //    }).asJava
   * TO:
   *     //    topKCollector.getTopK().asScala.flatMap { idWithScore =>
   *     //      vectorStore.get(idWithScore.id) match {
   *     //        case Some(kvector) =>
   *     //          Some(VectorResult(kvector.id, JsonObject.of("rid", kvector.rid, "meta", kvector.meta), score = Some(idWithScore.score)))
   *     //        case None =>
   *     //          // 打印一条警告，这样你就知道这种情况发生过，但不会让整个查询失败
   *     //          logger.warn(s"Vector with id=${idWithScore.id} was in TopK but not found during final fetch. It might have been deleted concurrently.")
   *     //          None // flatMap 会自动过滤掉 None，保证结果的完整性
   *     //      }
   *     //    }.asJava
   *
   * @param topKCollector initial candidates without further metadata
   * @param vectorStore store which contains more metadata of vectors
   * @return enriched result list
   */
  def convert(topKCollector: CompoundTopKCollector, vectorStore: KVectorStore): util.List[VectorResult] = {
    topKCollector.getTopK().asScala.flatMap { idWithScore =>
      vectorStore.get(idWithScore.id) match {
        case Some(kvector) =>
          Some(VectorResult(kvector.id, JsonObject.of("rid", kvector.rid, "meta", kvector.meta), score = Some(idWithScore.score)))
        case None =>
          // 打印一条警告，这样你就知道这种情况发生过，但不会让整个查询失败
          logger.warn(s"Vector with id=${idWithScore.id} was in TopK but not found during final fetch. It might have been deleted concurrently.")
          None // flatMap 会自动过滤掉 None，保证结果的完整性
      }
    }.asJava
  }

}