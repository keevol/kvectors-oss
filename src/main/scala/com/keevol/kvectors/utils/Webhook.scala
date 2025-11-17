package com.keevol.kvectors.utils

import io.vertx.core.json.JsonObject

import java.net.URL

object Webhook {
  def notify(endpoint: URL, payload: JsonObject): Unit = Http.postJson(endpoint, payload)
}