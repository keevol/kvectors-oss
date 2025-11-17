package com.keevol.kvectors.index.lsh

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

import com.keevol.kvectors.index.KVectorIndex

import java.util.concurrent.ConcurrentHashMap

/**
 * LSH index is different to other ones, we need to store dynamic index states as indexing is going on incrementally.
 *
 * if Euclidean LSH is needed, allocate a new  LSHIndex type to it later on.
 *
 * @param name index name to identify it
 * @param k LSH hyperplane number in a hash table
 * @param l hash table number to index and retrieve in parallel.
 * @param dimension vector dimension
 * @param hashers the initial hashers to hash vector at indexing and retrieval, this is the only immutable part once the index is created.
 */
case class HyperplanesLSHIndex(name: String, k: Int, l: Int, dimension: Int, hashers: Array[HyperplanesHasher]) extends KVectorIndex {
  override def getName: String = name

  /*
   * dynamic part of the LSH index at indexing and retrieval
   *
   * it's deemed to ba saved and loaded by index store at startup and shutdown.
   *
   * use `ConcurrentHashMap.keySet()` of type `ConcurrentHashMap.KeySetView<K,V>` as bucket list!
   */
  val lsh: Array[ConcurrentHashMap[LSHKey, ConcurrentHashMap[Long, Boolean]]] = new Array[ConcurrentHashMap[LSHKey, ConcurrentHashMap[Long, Boolean]]](hashers.length)
  lsh.indices.foreach(i => lsh(i) = new ConcurrentHashMap[LSHKey, ConcurrentHashMap[Long, Boolean]])

  // 这个逻辑可能得inline到具体的VectorCollection实现，毕竟， 我们的Index定义是纯粹的必要state，但LSH索引比较特殊，它可以增量实时构建和访问。
  // 这个时候，跟VectorStore并列处理可能更内聚？ ✅
}
