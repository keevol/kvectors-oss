package com.keevol.kvectors.admin

import com.keevol.kate.Kate
import com.keevol.keewebx.htmx.{HTMX, Hx}
import com.keevol.keewebx.templating.Jte
import com.keevol.keewebx.utils.{Handlers, WebResponse}
import com.keevol.kvectors.collections.AnnIndexKVectorCollection
import com.keevol.kvectors.enums.{CompressionStrategy, IndexStrategy}
import com.keevol.kvectors.lifecycles.Lifecycle
import com.keevol.kvectors.utils.JsonArrayToFloatArray
import com.keevol.kvectors.{KVectors, VectorMetadata, VectorRecord}
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.StaticHandler
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.lang3.{StringUtils, Strings}
import org.slf4j.LoggerFactory

import java.net.URI

class WebAdminServer(kdb: KVectors, host: String, port: Int) extends Lifecycle {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  private val webServer = new Kate((router: Router) => {
    router.errorHandler(404, ctx => ctx.response().end("not found :("))
    router.errorHandler(500, ctx => {

      ctx.response().end("oops..., my bad :(")
    })

    router.route("/").handler(ctx => {
      val vm = viewModel(kdb)
      WebResponse.html(ctx, Jte.render("admin/index.jte", vm))
    })
    router.get("/collection/add").handler(Handlers.chain(c => {
      val vm = viewModel(kdb)
      WebResponse.html(c, Jte.render("admin/addCollection.jte", vm))
    }))

    router.post("/collection/add").handler(Handlers.chain(c => {
      val name = StringUtils.trimToEmpty(c.request().getParam("collection-name"))
      val indexStrategy = StringUtils.trimToEmpty(c.request().getParam("index-strategy"))
      val compressStrategy = StringUtils.trimToEmpty(c.request().getParam("compress"))
      val inMemoryOrNot = StringUtils.trimToEmpty(c.request().getParam("in-memory"))
      if (StringUtils.isAnyEmpty(name, indexStrategy, compressStrategy, inMemoryOrNot)) {
        WebResponse.badRequest(c)
      } else {
        try {
          kdb.createVectorCollection(name, IndexStrategy.of(indexStrategy), CompressionStrategy.valueOf(StringUtils.upperCase(compressStrategy)), inMemory = if (Strings.CI.equals(inMemoryOrNot, "yes")) true else false)
          c.redirect(s"/collection/detail/${name}")
        } catch {
          case t: Throwable => {
            logger.warn(s"fails to create collection: ${name}: ${ExceptionUtils.getStackTrace(t)}")
            c.redirect(s"/")
          }
        }
      }
    }))
    router.get("/collection/detail/:name").handler(Handlers.chain(c => {
      val collectionName = c.pathParam("name")
      if (StringUtils.isEmpty(collectionName) || (!kdb.containsCollection(collectionName))) {
        logger.warn(s"invalid path param: `${collectionName}` tries to access collection detail page.")
        c.redirect("/")
      } else {
        val vm = viewModel(kdb)
        vm.put("collectionName", collectionName)
        val col = kdb.getCollection(collectionName)
        val colType = if (col.isDefined) {
          col.get.getClass.getSimpleName
        } else {
          "unknown"
        }
        vm.put("collectionType", colType)
        WebResponse.html(c, Jte.render("admin/collection.jte", vm))
      }
    }))

    router.get("/collection/vector/add").handler(Handlers.chain(c => {
      val collectionNameQueryParam = c.queryParam("collection")
      if (collectionNameQueryParam == null || collectionNameQueryParam.size() == 0) {
        c.redirect("/")
      } else {
        val collectionName = collectionNameQueryParam.get(0)
        val vm = viewModel(kdb)
        vm.put("collectionName", collectionName)
        WebResponse.html(c, Jte.render("admin/addVector.jte", vm))
      }
    }))
    router.post("/collection/vector/add").handler(Handlers.chain(c => {
      val r = c.request()
      val collectionName = StringUtils.trimToEmpty(r.getParam("collection-name"))
      val vector = StringUtils.trimToEmpty(r.getParam("vector"))
      val rid = StringUtils.trimToEmpty(r.getParam("rid"))
      val meta = StringUtils.trimToEmpty(r.getParam("meta"))
      logger.info(s"try to add vector to ${collectionName} with rid=${rid} and meta=${meta}")

      if (StringUtils.isAnyEmpty(collectionName, vector, rid)) {
        HTMX.triggerEvent(c, "oops", "请求参数不全 ！")
        //          HTMX.triggerEvent(c, "oops", "missing required argument(s)!")
        WebResponse.ok(c)
      } else {
        kdb.getCollection(collectionName) match {
          case None => {
            //              HTMX.triggerEvent(c, "oops", "请求的collection不存在，你丫是认真的吗？")
            HTMX.triggerEvent(c, "oops", "no such collection!")
            WebResponse.ok(c)
          }
          case Some(collection) => {
            collection.add(VectorRecord(JsonArrayToFloatArray(new JsonArray(vector)), VectorMetadata(rid, Some(if (StringUtils.isEmpty(meta)) new JsonObject() else new JsonObject(meta)))))
            HTMX.setTrigger(c, "goBackAfterSuccess")
            WebResponse.ok(c)
          }
        }
      }
    }))

    router.post("/collection/vector/query").handler(Handlers.chain(c => {
      val r = c.request()
      val collectionName = StringUtils.trimToEmpty(r.getParam("collectionName"))
      val vector = StringUtils.trimToEmpty(r.getParam("queryVector"))
      val topK = StringUtils.trimToEmpty(r.getParam("topK"))
      val score = StringUtils.trimToEmpty(r.getParam("score"))

      logger.info(s"name=${collectionName}, vector=${vector}, topK=${topK}, score=${score}")

      if (StringUtils.isAnyEmpty(collectionName, vector, topK, score)) {
        c.response().end("invalid vector query request")
      } else {
        kdb.getCollection(collectionName) match {
          case None => c.response().end("no such collection!")
          case Some(collection) => {
            val vectors = collection.query(JsonArrayToFloatArray(new JsonArray(vector)), topK.toInt, score.toFloat)
            WebResponse.html(c, Jte("admin/parts/vectorQueryResults.jte", vectors))
          }
        }
      }
    }))

    router.get("/collection/index/build").handler(Handlers.chain(c => {
      val collectionNameQueryParam = c.queryParam("collection")
      if (collectionNameQueryParam == null || collectionNameQueryParam.size() == 0) {
        c.redirect("/")
      } else {
        val collectionName = collectionNameQueryParam.get(0)
        val vm = viewModel(kdb)
        vm.put("collectionName", collectionName)
        WebResponse.html(c, Jte.render("admin/buildIndex.jte", vm))
      }
    }))
    router.post("/collection/index/build").handler(Handlers.chain(c => {
      val queryParams = c.queryParam("collection")
      if (queryParams.isEmpty) {
        logger.warn("no query parameter found on collection name")
        Hx.oops(c, "you bad")
      } else {
        val collectionName = StringUtils.trimToEmpty(queryParams.get(0))
        logger.info(s"build full index request received for collection:${collectionName}")
        if (StringUtils.isEmpty(collectionName) || (!kdb.containsCollection(collectionName))) {
          Hx.oops(c, "r u kidding me?")
        } else {
          val r = c.request()
          val enableIndexRightNowParam = StringUtils.trimToEmpty(r.getParam("enable-index-right-now"))
          val webhook = StringUtils.trimToEmpty(r.getParam("webhook"))
          val enableIndexRightNow = if (Strings.CI.equals(enableIndexRightNowParam, "yes")) true else false

          logger.info(s"build index request with enable right now=${enableIndexRightNow} and webhook='${webhook}'")

          val col = kdb.getCollection(collectionName).get
          if (!col.isInstanceOf[AnnIndexKVectorCollection]) {
            Hx.oops(c, "invalid collection type, no index build ability.")
          } else {
            col.asInstanceOf[AnnIndexKVectorCollection].buildFullIndexAsync(enableIndexRightNow, webhook = if (StringUtils.isNotEmpty(webhook)) Option(URI.create(webhook).toURL) else None)
            Hx.done(c, "构建全量索引任务已成功提交。")
          }
        }
      }
    }))

    router.post("/collection/index/build/history").handler(Handlers.chain(c => {
      val queryParams = c.queryParam("collection")
      if (queryParams.isEmpty) {
        logger.warn("no query parameter found on collection name")
        Hx.oops(c, "you bad")
      } else {
        val collectionName = StringUtils.trimToEmpty(queryParams.get(0))
        logger.info(s"build full index request received for collection:${collectionName}")
        if (StringUtils.isEmpty(collectionName) || (!kdb.containsCollection(collectionName))) {
          Hx.oops(c, "r u kidding me?")
        } else {
          val col = kdb.getCollection(collectionName).get
          if (!col.isInstanceOf[AnnIndexKVectorCollection]) {
            Hx.oops(c, "invalid collection type! (no index build ability also means no index build history)")
          } else {
            val logs = col.asInstanceOf[AnnIndexKVectorCollection].getIndexBuildHistory
            WebResponse.html(c, Jte("admin/parts/indexBuildLogs.jte", logs))
          }
        }
      }
    }))

    router.post("/collection/drop").handler(Handlers.chain(c => {
      val queryParams = c.queryParam("collection")
      if (queryParams.isEmpty) {
        logger.warn("no query parameter found on collection name")
        Hx.oops(c, "you bad")
      } else {
        val collectionName = StringUtils.trimToEmpty(queryParams.get(0))
        if (StringUtils.isEmpty(collectionName) || (!kdb.containsCollection(collectionName))) {
          Hx.oops(c, "r u kidding me?")
        } else {
          kdb.dropCollection(collectionName)
          HTMX.setTrigger(c, "goHomepageAfterMessaging")
          WebResponse.ok(c)
        }
      }
    }))

    router.route("/html").handler(ctx => {
      WebResponse.html(ctx, Jte.render("test.jte", new JsonObject().put("message", "mock message")))
    })

    router.route("/*").handler(StaticHandler.create()) // read from webroot under classpath as default.
    //      router.getRoutes.forEach(r => println(r.toString))
  }) {
    override def customizeHttpServerOptions(httpServerOptions: HttpServerOptions): Unit = {
      httpServerOptions.setMaxFormAttributeSize(1024 * 1024) // e.g., 1MB for form attributes
        .setMaxChunkSize(1024 * 1024);
    }
  }

  def getCollections(kdb: KVectors): JsonArray = {
    val collections = new JsonArray()
    kdb.listCollections().foreach(collections.add)
    collections
  }

  def viewModel(kdb: KVectors): JsonObject = {
    JsonObject.of("collections", getCollections(kdb))
  }

  override protected def doStart(): Unit = webServer.start(host, port)

  override protected def doStop(): Unit = webServer.stop()
}