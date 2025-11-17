package com.keevol.kvectors.api.webapi

import com.linecorp.armeria.common.{HttpResponse, MediaType}
import io.vertx.core.json.JsonObject

trait JsonResponse {
  def jsonResponse(json: JsonObject): HttpResponse = HttpResponse.of(MediaType.JSON, json.encode())
}