package com.keevol.kvectors.collections.creators

import com.keevol.kvectors.collections._
import com.keevol.kvectors.utils.TypeAlias.Dir
import io.vertx.core.json.JsonObject
import org.apache.commons.lang3.StringUtils

/**
 * This is mainly for admin API which will accept configuration from remote in json format.
 *
 * @param json configuration content which has been parsed into strong-typed JsonObject for later easy use.
 * @param dataDir kvectors' local data dir
 */
class JsonConfigKVectorCollectionsCreator(json: JsonObject, val dataDir: Dir) extends KVectorCollectionCreator {

  override def create(): KVectorCollection = {
    val collectionType = json.getString("type", "")
    if (StringUtils.isEmpty(collectionType)) {
      throw new IllegalArgumentException("illegal configuration without vector collection assigned.")
    }

    val collectionName = json.getString("name", "")
    if (StringUtils.isEmpty(collectionName)) {
      throw new IllegalArgumentException("no collection name configuration found which is a must.")
    }

    collectionType.toUpperCase match {
      case "TRANSIENT" =>
        new TransientKVectorCollection(collectionName)
      case "FLAT" =>
        var allInMemory = true
        if (json.containsKey("allInMemory")) {
          allInMemory = json.getBoolean("allInMemory")
        }
        new MapdbKVectorCollection(collectionName, dataDir, allInMemory = allInMemory)
      case "ANN" =>
        new AnnIndexKVectorCollection(collectionName, dataDir)
    }
  }
}