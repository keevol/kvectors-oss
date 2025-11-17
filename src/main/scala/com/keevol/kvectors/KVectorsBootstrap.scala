package com.keevol.kvectors

import com.keevol.goodies.Presets
import com.keevol.goodies.lifecycle.Shutdowns
import com.keevol.keewebx.KeewebxGlobals
import com.keevol.kvectors.admin.WebAdminServer
import com.keevol.kvectors.api.KVecAPIServer
import com.keevol.kvectors.utils.Closables
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.io.File

/**
 * set up a KVectors instance and its responding Web console for admin.
 */
object KVectorsBootstrap {
  private val logger = LoggerFactory.getLogger("KVectorsBootstrap")

  def main(args: Array[String]): Unit = {
    Presets.apply()

    val config = KeewebxGlobals.config.get()

    val dataDir = new File(config.get("data.dir"))
    if (!dataDir.exists()) {
      FileUtils.forceMkdir(dataDir)
    }

    val kdb = new KVectors(dataDir = dataDir)
    Shutdowns.add(() => Closables.closeWithLog(kdb))

    val webAdminEnabled = config.get("web.admin.enabled")
    if (StringUtils.isNotEmpty(webAdminEnabled) && webAdminEnabled.toBoolean) {
      val host = config.get("server.host")
      val port = config.get("server.port")
      val adminServer = new WebAdminServer(kdb, host, port.toInt)
      logger.info(s"start web admin server at ${host}:${port}")
      Shutdowns.add(() => adminServer.stop())
      adminServer.start()
      logger.info("KVectors web admin server started.")
    }

    val apiHost = config.get("api.server.host")
    val apiPort = config.get("api.server.port")
    val apiAccessToken = config.get("api.server.access.token")

    val apiServer = new KVecAPIServer(apiHost, apiPort.toInt, apiAccessToken, kdb)
    Shutdowns.add(() => apiServer.stop())
    apiServer.start()
    logger.info("KVectors api server is started.")

  }

}