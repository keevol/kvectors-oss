package com.keevol.kvectors.collections

import com.keevol.kvectors.collections.config.JsonConfig
import com.keevol.kvectors.utils.TypeAlias.Dir
import io.vertx.core.json.JsonObject
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils

import java.io.File
import java.nio.charset.StandardCharsets


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
trait ConfigInspectableVectorCollection {
  this: KVectorCollection =>

  def saveCreationConfig(): Unit

  def saveConfig(cfg: String, targetFile: File): Unit = {
    require(StringUtils.isNotEmpty(cfg))
    require(targetFile != null)
    FileUtils.writeStringToFile(targetFile, cfg, StandardCharsets.UTF_8, false)
  }

  def saveConfig(cfg: JsonObject, dataDir: Dir): Unit = {
    val config = if (!cfg.containsKey("type")) {
      JsonObject.of("type", getClass.getName, "args", cfg)
    } else {
      cfg
    }
    saveConfig(config.encode(), JsonConfig.getConfigFileAsPer(dataDir, collectionName()))
  }
}