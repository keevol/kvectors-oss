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

import com.keevol.kvectors.ops.KVectorMetrics
import com.keevol.kvectors.{KVectors, VectorMetadata, VectorRecord}
import com.keevol.kvectors.utils.{JsonArrayToFloatArray, With}
import com.linecorp.armeria.common.{HttpResponse, HttpStatus}
import com.linecorp.armeria.server.ServiceRequestContext
import com.linecorp.armeria.server.annotation.{Param, Post, Put}
import io.vertx.core.json.{JsonArray, JsonObject}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory

class VectorCollectionRoutes(kdb: KVectors) extends JsonResponse {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  private val qTimer = KVectorMetrics.timer(getClass.getName + "#query")
  private val addTimer = KVectorMetrics.timer(getClass.getName + "#add")

  /**
   * query with post since we need json to post the query vector content.
   */
  @Post("/collections/{name}")
  def querySimilarVectors(body: String, @Param("name") name: String, ctx: ServiceRequestContext): HttpResponse = {
    With(qTimer.time()) {
      try {
        require(kdb.containsCollection(name), s"the collection with name: ${name} must exists")

        val json = new JsonObject(body)
        require(json.containsKey("vector"))

        val vectorArray = json.getJsonArray("vector")
        val vector = JsonArrayToFloatArray(vectorArray)

        val vectors = new JsonArray()
        kdb.getCollection(name).get.query(vector).forEach(vr => {
          vectors.add(JsonObject.of("id", vr.id, "score", vr.score.get, "meta", vr.meta))
        })
        jsonResponse(JsonObject.of("candidates", vectors))
      } catch {
        case t: Throwable => {
          logger.warn(ExceptionUtils.getStackTrace(t))
          HttpResponse.of(HttpStatus.BAD_REQUEST)
        }
      }
    }
  }

  /**
   * add vector with json schema:
   * {
   *   "vector": [],
   *   "rid": "",
   *   "metadata": {
   *   }
   * }
   */
  @Put("/collections/{name}")
  def addVector(body: String, @Param("name") name: String, ctx: ServiceRequestContext): HttpResponse = {
    With(addTimer.time()) {
      try {
        require(kdb.containsCollection(name), s"the collection with name: ${name} must exists")

        val json = new JsonObject(body)
        require(json.containsKey("vector"))
        require(json.containsKey("rid"))

        val vectorArray = json.getJsonArray("vector")
        val vector = JsonArrayToFloatArray(vectorArray)
        val rid = json.getString("rid")
        val metadata = json.getString("metadata", "{}")
        kdb.getCollection(name).get.add(VectorRecord(vector, VectorMetadata(rid, Option(new JsonObject(metadata)))))
        jsonResponse(JsonObject.of("message", "added!"))
      } catch {
        case t: Throwable => {
          logger.warn(ExceptionUtils.getStackTrace(t))
          HttpResponse.of(HttpStatus.BAD_REQUEST)
        }
      }
    }
  }

}