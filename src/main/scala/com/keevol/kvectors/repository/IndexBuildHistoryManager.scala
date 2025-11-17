package com.keevol.kvectors.repository

import com.keevol.kvectors.utils.{Closables, MapDB}
import org.mapdb.{DB, Serializer}

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
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer

/**
 * separate index build history concern into standalone manager
 */
trait IndexBuildHistoryManager {
  /**
   * add index building log
   *
   * @param ts timestamp for message
   * @param message building message
   */
  def add(ts: Long, message: String): Unit

  /**
   * return latest history logs
   * @param n the result entry number
   * @return logs of index building history
   */
  def list(n: Int = 20): List[String]

}

class MapDBackendIndexBuildHistoryManager(dbFile: File) extends IndexBuildHistoryManager with AutoCloseable {
  private val mdb: DB = MapDB(dbFile)
  private val indexBuildHistoryStore = mdb.treeMap("index.build.history", Serializer.LONG, Serializer.STRING).createOrOpen()

  /**
   * add index building log
   *
   * @param ts timestamp for message
   * @param message building message
   */
  override def add(ts: Long, message: String): Unit = indexBuildHistoryStore.put(ts, message)

  /**
   * return latest history logs
   *
   * @param n the result entry number
   * @return logs of index building history
   */
  override def list(n: Int): List[String] = {
    val listBuffer = new ListBuffer[String]
    val iter = indexBuildHistoryStore.descendingEntryIterator()
    val counter = new AtomicInteger(0)
    while (iter.hasNext && counter.getAndIncrement() < n) {
      val e = iter.next()
      listBuffer.append(s"${e.getKey}, ${e.getValue}")
    }
    listBuffer.toList
  }

  override def close(): Unit = Closables.closeWithLog(mdb)
}




