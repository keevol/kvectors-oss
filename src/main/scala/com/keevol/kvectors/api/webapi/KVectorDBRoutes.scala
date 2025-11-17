package com.keevol.kvectors.api.webapi

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

import com.keevol.kvectors.KVectors
import com.keevol.kvectors.collections.creators.JsonConfigKVectorCollectionsCreator
import com.keevol.kvectors.enums.{CompressionStrategy, IndexStrategy, SimilarityAlg}
import com.linecorp.armeria.common.{HttpResponse, HttpStatus}
import com.linecorp.armeria.server.ServiceRequestContext
import com.linecorp.armeria.server.annotation.{Get, Post, Put}
import io.vertx.core.json.{JsonArray, JsonObject}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory


class KVectorDBRoutes(kdb: KVectors) extends JsonResponse {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  @Get("/collections")
  def listCollections(): HttpResponse = {
    val ja = new JsonArray()
    kdb.listCollections().foreach(c => ja.add(c))
    jsonResponse(JsonObject.of("collections", ja))
  }


  /**
   * This is the v1 api endpoint to create a vector collection which is controlled by serverside and limited to 3 fixed vector collection type.
   *
   * <pre>
   *{
   *"name": "collectionName",
   *"index-type": "",
   *"compression": "",
   *"similarity-alg": "",
   *"transient": false
   *}
   * </pre>
   */
  @Post("/collections")
  def addCollection(body: String, ctx: ServiceRequestContext): HttpResponse = {
    try {
      val json = new JsonObject(body)
      val collectionName = StringUtils.trimToEmpty(json.getString("name"))
      require(StringUtils.isNotEmpty(collectionName))

      val indexTypeValue = StringUtils.trimToEmpty(json.getString("index-type"))
      val indexStrategy = if (StringUtils.isEmpty(indexTypeValue)) {
        IndexStrategy.NO_INDEX
      } else {
        IndexStrategy.valueOf(indexTypeValue.toUpperCase)
      }

      val compressionValue = StringUtils.trimToEmpty(json.getString("compression"))
      val compressionStrategy = if (StringUtils.isEmpty(compressionValue)) {
        CompressionStrategy.NO
      } else {
        CompressionStrategy.valueOf(compressionValue.toUpperCase())
      }

      val similarityValue = StringUtils.trimToEmpty(json.getString("similarity-alg"))
      val similarityAlg = if (StringUtils.isEmpty(similarityValue)) {
        SimilarityAlg.COSINE
      } else {
        SimilarityAlg.valueOf(similarityValue.toUpperCase())
      }

      val transient: Boolean = if (json.containsKey("transient")) json.getBoolean("transient") else false

      ctx.blockingTaskExecutor().execute(() => {
        kdb.createVectorCollection(collectionName, indexStrategy = indexStrategy, compressionStrategy = compressionStrategy, similarityAlg = similarityAlg, inMemory = transient)
      })

      jsonResponse(JsonObject.of("message", "ok"))
    } catch {
      case t: Throwable => {
        logger.error(s"something goes wrong when create vector collection: ${ExceptionUtils.getStackTrace(t)}")
        HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR)
      }
    }
  }

  /**
   * This is v2 but ok with same endpoint url, since v1 is POST and v2 is PUT.
   *
   * v2 will accept new json-format vector collection creation configuration which conforms to specific vector collection implementation type.
   *
   * v1 just accept coarse-grained configuration for several vector collection types.
   *
   * @param body configuration content which should be in json format
   * @param ctx request context
   * @return result http response
   */
  @Put("/collections")
  def createCollection(body: String, ctx: ServiceRequestContext): HttpResponse = {
    try {
      val json = new JsonObject(body)

      ctx.blockingTaskExecutor().execute(() => {
        kdb.createVectorCollection(new JsonConfigKVectorCollectionsCreator(json, dataDir = kdb.dataDir))
      })

      jsonResponse(JsonObject.of("message", "ok"))
    } catch {
      case t: Throwable => {
        logger.error(s"something goes wrong when create vector collection: ${ExceptionUtils.getStackTrace(t)}")
        HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR)
      }
    }
  }

}

