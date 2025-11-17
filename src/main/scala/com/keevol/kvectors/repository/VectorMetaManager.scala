package com.keevol.kvectors.repository

import com.keevol.kvectors.utils.Closables
import io.vertx.core.json.JsonObject

import java.io.{File, RandomAccessFile}
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.ConcurrentHashMap


trait VectorMetaManager {
  /**
   * 原则上meta内容是必须的，否则， 最终没有元信息将当前向量与向量对应的原始内容相关联。
   *
   * @param id   implicit id in kvectors
   * @param meta to connect vector and original information chunk together.
   */
  def add(id: Long, meta: JsonObject): Unit

  def update(id: Long, meta: JsonObject): Unit

  def get(id: Long): Option[JsonObject]

  def iterator(): util.Iterator[(Long, Option[JsonObject])]

  def idOf(rid: String): Option[Long]

  def delete(id: Long): Unit

  def dimension(dim: Int): Unit

  def dim(): Option[Int]
}

//
//class OnHeapMetaManager extends MetaManager {
//
//  private val cache = new ConcurrentHashMap[Long, JsonObject]()
//
//  override def add(id: Long, meta: JsonObject): Unit = cache.put(id, meta)
//
//  override def get(id: Long): Option[JsonObject] = {
//    if (cache.containsKey(id)) {
//      Some(cache.get(id))
//    } else {
//      None
//    }
//  }
//
//  override def all(): util.List[JsonObject] = {
//    val list = new util.ArrayList[JsonObject]()
//    cache.entrySet().forEach(e => {
//      val id = e.getKey
//      val v: JsonObject = e.getValue
//      v.put("_id", id)
//      list.add(v)
//    })
//    list
//  }
//}
//
//class OnDiskMetaManager(metaFile: File) extends OnHeapMetaManager with AutoCloseable {
//
//  private val writer = new RandomAccessFile(metaFile, "rw")
//
//  def reload(): Unit = {
//    while (writer.getFilePointer < writer.length()) {
//      val id = writer.readLong()
//      val frameLen = writer.readInt()
//      if (frameLen > 0) {
//        val buffer = new Array[Byte](frameLen)
//        val readLen = writer.read(buffer)
//        require(frameLen == readLen)
//        super.add(id, new JsonObject(new String(buffer, StandardCharsets.UTF_8)))
//      }
//    }
//  }
//
//  override def add(id: Long, meta: JsonObject): Unit = {
//    super.add(id, meta)
//    writer.writeLong(id)
//    if (meta == null || meta.isEmpty) {
//      writer.writeInt(0)
//    } else {
//      val payload = meta.encode().getBytes(StandardCharsets.UTF_8)
//      writer.writeInt(payload.length)
//      writer.write(payload)
//    }
//  }
//
//  override def close(): Unit = {
//    Closables.closeWithLog(writer)
//  }
//}

