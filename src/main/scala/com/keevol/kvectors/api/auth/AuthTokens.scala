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

import org.apache.commons.lang3.{StringUtils, Strings}

object AuthTokens {
  private val bearerPrefix: String = "Bearer "
  val exampleTokenForDebug: String = "keepEvolution"

  def bearerToken2HeaderValue(token: String): String = s"${bearerPrefix}${StringUtils.trimToEmpty(token)}"

  def bearerTokenFromHeaderValue(headerValue: String): Option[String] = {
    if (!Strings.CI.contains(headerValue, bearerPrefix)) {
      return None
    }

    // I know Option(x) way, but I would like writing it the following way
    val tokenValue = StringUtils.trimToEmpty(StringUtils.substringAfter(headerValue, bearerPrefix))
    if (StringUtils.isEmpty(tokenValue)) {
      return None
    }
    Some(tokenValue)
  }
}