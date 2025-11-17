package com.keevol.kvectors.repository

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

import com.keevol.kvectors.utils.{Closables, MapDB}
import io.vertx.core.json.JsonObject
import org.apache.commons.lang3.StringUtils
import org.mapdb.{DB, Serializer}

import java.io.File
import java.util
import scala.collection.JavaConverters._

/**
 * use mapdb to store vector metadata
 *
 * @param dbFile the mapdb storage file
 */
class MapdbVectorMetaManager(dbFile: File) extends VectorMetaManager with AutoCloseable {
  // use mapdb as metadata storage, although we can write to file directly too.
  protected val metaDB: DB = MapDB(dbFile)
  // save metadata as per auto-increment id
  private val metaStore = metaDB.hashMap("meta.store", Serializer.LONG, Serializer.STRING).createOrOpen()
  // id mapping between rid(business id) and auto-increment id, id means nothing to users, only rid is meaningful to users.
  private val idMapping = metaDB.hashMap("rid2id", Serializer.STRING, Serializer.LONG).createOrOpen()

  private val dimensionValue = metaDB.atomicInteger("dimension.val").createOrOpen()

  override def idOf(rid: String): Option[Long] = {
    val id = idMapping.get(rid)
    if (id == null) None else Some(id)
  }

  /**
   * 原则上meta内容是必须的，否则， 最终没有元信息将当前向量与向量对应的原始内容相关联。
   *
   * @param id   implicit id in kvectors
   * @param meta to connect vector and original information chunk together.
   */
  override def add(id: Long, meta: JsonObject): Unit = {
    require(meta != null, "meta object is null")
    require(meta.containsKey("rid"), "rid is a must in meta")
    val rid = meta.getString("rid")
    metaStore.put(id, meta.encode())
    idMapping.put(rid, id)
  }

  override def update(id: Long, meta: JsonObject): Unit = {
    require(meta != null)
    require(meta.containsKey("rid"))
    val rid = meta.getString("rid")
    idOf(rid) match {
      case Some(expectedId) => require(expectedId == id)
      case None => throw new IllegalStateException("rid is not match!")
    }
    metaStore.put(id, meta.encode())
  }

  override def delete(id: Long): Unit = {
    get(id) match {
      case Some(jsonObject) =>
        val rid = jsonObject.getString("rid")
        idMapping.remove(rid)
        metaStore.remove(id)
      case None => // do nothing
    }
  }

  override def get(id: Long): Option[JsonObject] = {
    val meta = metaStore.get(id)
    if (StringUtils.isEmpty(meta)) {
      None
    } else {
      Some(new JsonObject(meta))
    }
  }

  override def iterator(): util.Iterator[(Long, Option[JsonObject])] = {
    new util.Iterator[(Long, Option[JsonObject])] {
      private val idIterator = metaStore.keySet().iterator()

      override def hasNext: Boolean = idIterator.hasNext

      override def next(): (Long, Option[JsonObject]) = {
        val id = idIterator.next()
        (id, get(id))
      }
    }
  }

  override def dimension(dim: Int): Unit = dimensionValue.set(dim)

  override def dim(): Option[Int] = Option(dimensionValue.get())

  override def close(): Unit = Closables.closeWithLog(metaDB)

}

/**
 * except for vector metadata, we also need to store index building history
 *
 * @param dbFile the mapdb storage file
 */
class AnnIndexCollectionMetaManager(dbFile: File) extends MapdbVectorMetaManager(dbFile) {
  // timestamp => build log, we need the order of the entries, so treemap instead of hashmap, low frequency access
  private val indexBuildHistoryStore = metaDB.treeMap("index.build.history", Serializer.LONG, Serializer.STRING).createOrOpen()

  def addLog(ts: Long, message: String): Unit = indexBuildHistoryStore.put(ts, message)

  def getIndexBuildHistory: Array[String] = indexBuildHistoryStore.values().asScala.toArray
}