package com.keevol.kvectors.utils

import io.vertx.core.json.JsonObject

import java.net.URL
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import java.nio.charset.StandardCharsets
import java.time.Duration

object Http {
  private val httpClient: HttpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).connectTimeout(Duration.ofSeconds(30)).build();
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = httpClient.close()
  })

  def postJson(url: URL, json: JsonObject): String = {
    val requestBuilder = HttpRequest.newBuilder().uri(url.toURI).POST(BodyPublishers.ofString(json.encode(), StandardCharsets.UTF_8)).header("Content-Type", "application/json")
    requestBuilder.uri(url.toURI)
    val response = httpClient.send(requestBuilder.build(), BodyHandlers.ofString(StandardCharsets.UTF_8))
    if (response.statusCode() == 200) {
      response.body()
    } else {
      throw new Exception(s"${response.request().method().toUpperCase()} request to [${response.request().uri()}] fails with status=${response.statusCode()} and body=${response.body()}")
    }
  }

}