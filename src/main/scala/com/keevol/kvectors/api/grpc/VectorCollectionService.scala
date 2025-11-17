package com.keevol.kvectors.api.grpc

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

import com.keevol.kvectors.{KVectors, VectorMetadata, VectorRecord}
import com.keevol.kvectors.api.grpc.utils.GrpcSafe
import com.keevol.kvectors.grpc.{AddVectorRequest, AddVectorResponse, KVectorCollectionServiceGrpc, VectorQueryRequest, VectorQueryResponse, VectorQueryResult}
import com.keevol.kvectors.utils.With
import io.grpc.stub.StreamObserver
import io.vertx.core.json.JsonObject
import org.apache.commons.lang3.{ArrayUtils, StringUtils}
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

class VectorCollectionService(kdb: KVectors) extends KVectorCollectionServiceGrpc.KVectorCollectionServiceImplBase {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  import com.keevol.kvectors.ops.KVectorMetrics._

  private val t = timer(getClass.getName + "#addVector")
  private val qt = timer(getClass.getName + "#query")

  /**
   */
  override def addVector(request: AddVectorRequest, responseObserver: StreamObserver[AddVectorResponse]): Unit = {
    With(t.time()) {
      GrpcSafe(responseObserver) {
        val collectionName = request.getCollection
        val vector = request.getVectorList.asScala.map(_.floatValue()).toArray
        val rid = request.getRid
        val meta = request.getMeta

        require(StringUtils.isNotEmpty(collectionName) && ArrayUtils.isNotEmpty(vector) && StringUtils.isNotEmpty(rid))

        val metadata = if (StringUtils.isEmpty(meta)) None else Some(new JsonObject(meta))
        kdb.getCollection(collectionName).foreach(vc => vc.add(VectorRecord(vector = vector, VectorMetadata(rid, metadata))))
        AddVectorResponse.newBuilder().setMessage("added!").build()
      }
    }
  }

  /**
   */
  override def query(request: VectorQueryRequest, responseObserver: StreamObserver[VectorQueryResponse]): Unit = {
    With(qt.time()) {
      GrpcSafe(responseObserver) {
        val resultBuilder = VectorQueryResponse.newBuilder()

        val collectionName = request.getCollection
        val vector = request.getVectorList.asScala.map(_.floatValue()).toArray
        val topK = if (request.getTopK <= 0) 3 else request.getTopK
        val threshold = if (request.getThreshold <= 0) 0.8f else request.getThreshold

        require(StringUtils.isNotEmpty(collectionName) && ArrayUtils.isNotEmpty(vector))


        val col = kdb.getCollection(collectionName)
        if (col.isEmpty) {
          throw new IllegalArgumentException(s"no such collection: ${collectionName}")
        }
        col.get.query(vector, topK, threshold).forEach(result => {
          logger.info(s"result: ${result}")
          resultBuilder.addResults(VectorQueryResult.newBuilder().setId(result.id).setScore(result.score.get).setMeta(result.meta.encode()).build())
        })
        resultBuilder.build()
      }
    }
  }
}