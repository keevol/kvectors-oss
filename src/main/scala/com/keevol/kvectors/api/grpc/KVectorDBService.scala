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

import com.keevol.kvectors.KVectors
import com.keevol.kvectors.enums.{CompressionStrategy, IndexStrategy, SimilarityAlg}
import com.keevol.kvectors.grpc._
import io.grpc.stub.StreamObserver

class KVectorDBService(kdb: KVectors) extends KVectorsDBServiceGrpc.KVectorsDBServiceImplBase {
  /**
   */
  override def listVectorCollections(request: ListCollectionRequest, responseObserver: StreamObserver[ListCollectionResponse]): Unit = {
    try {
      val builder = ListCollectionResponse.newBuilder()
      kdb.listCollections().foreach(colName => {
        builder.addCollection(colName)
      })
      responseObserver.onNext(builder.build())
    } catch {
      case t: Throwable => responseObserver.onError(t)
    } finally {
      responseObserver.onCompleted()
    }
  }

  /**
   */
  override def createVectorCollection(request: CreateCollectionRequest, responseObserver: StreamObserver[CreateCollectionResponse]): Unit = {
    try {
      val collectionName = request.getName
      val indexStrategy = request.getIndexType.getNumber match {
        case 0 => IndexStrategy.NO_INDEX
        case 1 => IndexStrategy.ANN
      }
      val compressionStrategy = request.getCompress.getNumber match {
        case 0 => CompressionStrategy.NO
        case 1 => CompressionStrategy.ZSTD
        case 2 => CompressionStrategy.LZ4
      }
      val similarityAlg = request.getSimilarityAlg.getNumber match {
        case 0 => SimilarityAlg.COSINE
        case 1 => SimilarityAlg.DOT_PRODUCT
        case 2 => SimilarityAlg.EUCLIDEAN
      }
      val transient = request.getTransient

      kdb.createVectorCollection(collectionName, indexStrategy = indexStrategy, compressionStrategy = compressionStrategy, similarityAlg = similarityAlg, inMemory = transient)

      responseObserver.onNext(CreateCollectionResponse.newBuilder().setMessage("done!").build())
    } catch {
      case t: Throwable => responseObserver.onError(t)
    } finally {
      responseObserver.onCompleted()
    }
  }
}