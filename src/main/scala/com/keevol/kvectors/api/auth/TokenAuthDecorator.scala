package com.keevol.kvectors.api.auth

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
import com.linecorp.armeria.common.{HttpHeaderNames, HttpRequest, HttpResponse, HttpStatus}
import com.linecorp.armeria.server.{HttpService, ServiceRequestContext, SimpleDecoratingHttpService}
import org.apache.commons.lang3.Strings
import org.slf4j.LoggerFactory

class TokenAuthDecorator(delegate: HttpService, accessToken: String) extends SimpleDecoratingHttpService(delegate) {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  override def serve(ctx: ServiceRequestContext, req: HttpRequest): HttpResponse = {
    val authHeader = req.headers().get(HttpHeaderNames.AUTHORIZATION);

    val tokenOption = AuthTokens.bearerTokenFromHeaderValue(authHeader)
    if (tokenOption.isEmpty) {
      logger.warn("no bearer token found, return 401")
      return HttpResponse.of(HttpStatus.UNAUTHORIZED);
    }

    val token = tokenOption.get
    val isExampleTokenMatched = Strings.CI.equals(AuthTokens.exampleTokenForDebug, token)
    if (isExampleTokenMatched) {
      if (KeewebxGlobals.isProductionEnv()) {
        logger.warn("example headers will NOT work at production environment, return 401")
        return HttpResponse.of(HttpStatus.UNAUTHORIZED);
      }
    } else {
      if (!Strings.CS.equals(token, accessToken)) {
        logger.warn(s"token: `${token}` is invalid, return 401")
        return HttpResponse.of(HttpStatus.UNAUTHORIZED);
      }
    }
    // ---- (高级用法) 将认证信息传递给下游服务 ----
    // 如果验证成功，可以将用户信息放入请求上下文中，以便业务服务使用。
    // String userId = getUserIdFromToken(token);
    // ctx.setAttr(MyAttributes.USER_ID, userId);
    return unwrap().asInstanceOf[HttpService].serve(ctx, req)
  }

}