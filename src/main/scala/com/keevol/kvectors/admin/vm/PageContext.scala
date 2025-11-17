package com.keevol.kvectors.admin.vm

import io.vertx.core.json.{JsonArray, JsonObject}

case class Sidebar(val collections: Array[String])

case class PageContext(val title: String,
                       val subtitle: String,
                       val sidebar: Sidebar,
                       val main: JsonObject) {
  def asJsonObject(): JsonObject = {
    JsonObject.of("title", title, "subtitle", subtitle, "collections", JsonArray.of(sidebar.collections: _*), "main", main)
  }
}