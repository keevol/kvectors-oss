package com.keevol.kvectors.collections.loader

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

import com.keevol.kvectors.collections.KVectorCollection
import com.keevol.kvectors.collections.config.JsonConfig
import com.keevol.kvectors.utils.TypeAlias._
import io.vertx.core.json.JsonObject
import org.apache.commons.lang3.Strings
import org.slf4j.LoggerFactory

import scala.reflect.{ClassTag, classTag}

/**
 * load a KVectorCollection if the current dir has a proper configuration for a KVectorCollection.
 *
 * we inline config saving and applying in each KVectorCollection by attaching them with ConfigInspectableVectorCollection trait and their companion object.
 *
 * This way, the config management parts can align as near as possible, make them easy to tackle with.
 *
 * @see [[com.keevol.kvectors.collections.IVFIndexedKVectorCollection]] or [[com.keevol.kvectors.collections.IVFRaBitQKVectorCollection]] companion object as demo
 */
abstract class KVectorCollectionLoader[C <: KVectorCollection : ClassTag] extends PartialFunction[Dir, C] {
  protected final val logger = LoggerFactory.getLogger(getClass.getName)

  override def isDefinedAt(collectionDir: Dir): Boolean = {
    val configFile = JsonConfig.getConfigFile(collectionDir)
    logger.info(s"isDefinedAt ${configFile}? ${configFile.exists()}")
    if (!configFile.exists()) {
      logger.info(s"config file not exist: ${configFile}, return false directly")
      return false
    }
    val json = JsonConfig.from(configFile)
    if (!json.containsKey("type")) {
      logger.info(s"no config key `type` found in config file: ${configFile}!")
      return false
    }
    val classTagForC = classTag[C]
    val expectedClassName = classTagForC.runtimeClass.getName
    logger.info(s"Expected class name for comparison is: [${expectedClassName}]")

    Strings.CS.equals(json.getString("type"), expectedClassName)
  }


  override def apply(collectionDir: Dir): C = {
    val configFile = JsonConfig.getConfigFile(collectionDir)
    val config = JsonConfig.from(configFile)
    apply(config)
  }

  def apply(config: JsonObject): C
}