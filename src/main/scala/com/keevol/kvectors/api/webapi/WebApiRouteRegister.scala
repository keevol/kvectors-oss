package com.keevol.kvectors.api.webapi

import com.linecorp.armeria.common.{HttpResponse, MediaType}
import com.linecorp.armeria.server.ServiceBindingBuilder
import io.vertx.core.json.JsonObject

import java.util.function.Consumer

/**
 * make it type-safer for webapi
 *
 * annotated service is preferred since only it has docs service support
 *<pre>
 *     \@Get("/api/users/{id}   "   )
 *     public HttpResponse getUser(@Param("id") String id) {
 *         return HttpResponse.of("User: %s", id);
 *     }
 *     </pre>
 */
trait WebApiRouteRegister extends Consumer[ServiceBindingBuilder] {
  override def accept(builder: ServiceBindingBuilder): Unit = {
    apply(builder)
  }

  def apply(builder: ServiceBindingBuilder): Unit

  def jsonResponse(json: JsonObject): HttpResponse = HttpResponse.of(MediaType.JSON, json.encode())
}

/**
 * A demo impl.
 *
 * use it by registering it with `serverBuilder.withRoute(helloWebApi)`
 *
 * prefer annotated version instead this !!!
 * Although this is more like vert.x style.
 */
class HelloWebApi extends WebApiRouteRegister {

  override def apply(builder: ServiceBindingBuilder): Unit = {
    builder.get("/hello").build((ctx, req) => {
      HttpResponse.of("hello")
    })
    builder.get("/hello2").build((ctx, req) => {
      jsonResponse(JsonObject.of("type", "json response"))
    })
  }
}