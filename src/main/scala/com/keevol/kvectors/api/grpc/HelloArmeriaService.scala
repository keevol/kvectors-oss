package com.keevol.kvectors.api.grpc

import com.keevol.kvectors.grpc.{HelloReply, HelloRequest, HelloServiceGrpc}
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

class HelloArmeriaService extends HelloServiceGrpc.HelloServiceImplBase {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * test with:
   *
   * <pre>
   *   grpcurl -plaintext -d '{"name": "kvectors"}' 127.0.0.1:1980 com.keevol.kvectors.grpc.HelloService/Hello
   *   </pre>
   * @param request
   * @param responseObserver
   */
  override def hello(request: HelloRequest, responseObserver: StreamObserver[HelloReply]): Unit = {
    val name = request.getName
    val reply = HelloReply.newBuilder().setMessage(s"hello, ${name}").build()
    responseObserver.onNext(reply)
    responseObserver.onCompleted()
  }
}