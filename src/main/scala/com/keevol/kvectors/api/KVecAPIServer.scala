package com.keevol.kvectors.api

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

import com.keevol.keewebx.KeewebxGlobals
import com.keevol.kvectors.KVectors
import com.keevol.kvectors.api.auth.{AuthTokens, TokenAuthDecorator}
import com.keevol.kvectors.api.grpc.{HelloArmeriaService, KVectorDBService, VectorCollectionService}
import com.keevol.kvectors.api.webapi.{HelloWebApi, KVectorDBRoutes, VectorCollectionRoutes}
import com.keevol.kvectors.lifecycles.Lifecycle
import com.linecorp.armeria.common.{HttpHeaderNames, HttpHeaders, HttpResponse, SessionProtocol}
import com.linecorp.armeria.server.Server
import com.linecorp.armeria.server.docs.DocService
import com.linecorp.armeria.server.grpc.GrpcService
import com.linecorp.armeria.server.logging.LoggingService
import io.grpc.protobuf.services.ProtoReflectionServiceV1

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference

class KVecAPIServer(host: String, port: Int, apiAccessToken: String, kdb: KVectors) extends Lifecycle {

  private val apiServerEndpoint: AtomicReference[Server] = new AtomicReference[Server]()

  private val sb = Server.builder()
  //  sb.port(new InetSocketAddress(host, port), SessionProtocol.PROXY, SessionProtocol.HTTP)
  sb.http(new InetSocketAddress(host, port)) // sb.port和sb.http不要混用，否则作用域会重叠，建议都是走sb.http配置块, 同样sb，但不同的sb.http配置块相当于不同的虚拟主机。
  // setup web api endpoints
  //  sb.service("/", (ctx, req) => HttpResponse.of("""Hello, KVectors API Users. You can go to <a href="/docs">/docs</a> for apidoc.""")) // ONLY for simple demo
  //  sb.withRoute(new HelloWebApi()) // web api should go this way which has more encapsulation.
  sb.service("/", (ctx, req) => HttpResponse.ofRedirect("/docs"))
  sb.annotatedService("/api", new KVectorDBRoutes(kdb), Array(): _*)
  sb.annotatedService("/api", new VectorCollectionRoutes(kdb), Array(): _*) // 第二个参数 Array():_* 纯粹是傻逼scala2.12的限制，对重载支持不好，迫不得已啊！
  sb.routeDecorator.pathPrefix("/api").build(delegate => new TokenAuthDecorator(delegate, apiAccessToken))

  // setup grpc endpoints
  sb.service(
    GrpcService.builder()
      .addService(new HelloArmeriaService())
      .addService(new KVectorDBService(kdb))
      .addService(new VectorCollectionService(kdb))
      .addService(ProtoReflectionServiceV1.newInstance()) // server reflection
      .build(),
    LoggingService.newDecorator(), delegate => new TokenAuthDecorator(delegate, apiAccessToken)
  )

  // setup docs endpoint at dev/test stages only
  if (!KeewebxGlobals.isProductionEnv()) {
    sb.serviceUnder("/docs", DocService.builder()
      .injectedScripts("""document.querySelector("header div h6 span").innerText = "KVectors API Document"; """)
      .exampleHeaders(HttpHeaders.of(HttpHeaderNames.AUTHORIZATION, AuthTokens.bearerToken2HeaderValue(AuthTokens.exampleTokenForDebug)))
      .build())
  }
  // finish server build
  apiServerEndpoint.set(sb.build())

  override protected def doStart(): Unit = {
    apiServerEndpoint.get().start().join()
  }

  override protected def doStop(): Unit = {
    apiServerEndpoint.get().stop()
  }
}