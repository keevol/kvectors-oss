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

import com.keevol.goodies.annotations.AliasFor
import com.keevol.kvectors.lifecycles.Lifecycle
import com.keevol.kvectors.{KVector, KVectorLite}

import java.util.concurrent.atomic.AtomicLong

/**
 * consistent abstraction to a vector store
 *
 * A soft limit in this design is I let the vector id to be auto-increment, but it may not be always the case?!
 *
 */
trait KVectorStore extends Lifecycle {

  /**
   * save a vector to store
   *
   * @param vector to save
   */
  def add(vector: KVector): Unit

  /**
   * get the dimension of vectors in store.
   *
   * @return vector dimension, 0 or real dimension
   */
  def dimension: Int

  /**
   * last vector id which helps us to determine an approximate count,
   * for example, to build an IVF index, we need to choose a proper K value which usually equals to sqrt of total count, this don't need to be exact.
   *
   * @return last vector id
   */
  def last(): Long

  /**
   * get a full vector as per vector id
   *
   * @param id of vector
   * @return a full vector
   */
  def get(id: Long): Option[KVector]

  /**
   * mainly for indexing retrieval only, it will not trigger meta store access which is not necessary.
   *
   * @param id vector id
   * @return a vector with id and value
   */
  def value(id: Long): Option[KVectorLite]


  def keys(): Iterator[Long] = new Iterator[Long] {
    private val counter = new AtomicLong(0)

    override def hasNext: Boolean = counter.get() < last()

    override def next(): Long = counter.getAndIncrement()
  }

  /**
   * in case we want to get vectors after some id (partial indexed + partial not indexed)
   *
   * @param id the filter id to use
   * @return vectors with id after the parameter
   */
  def after(id: Long): Iterator[Option[KVectorLite]] = new Iterator[Option[KVectorLite]] {
    private val counter = new AtomicLong(id)

    override def hasNext: Boolean = counter.get() < last()

    override def next(): Option[KVectorLite] = {
      val nextId = counter.incrementAndGet()
      value(nextId)
    }
  }

  /**
   * get all vectors in the store. Return an iterator to avoid huge memory consumption.
   *
   * @return an iterator of all vectors
   */
  def scan(): Iterator[Option[KVectorLite]] = new Iterator[Option[KVectorLite]] {

    private val keysIterator = keys()

    override def hasNext: Boolean = keysIterator.hasNext

    override def next(): Option[KVectorLite] = {
      val id = keysIterator.next()
      value(id)
    }
  }

  @AliasFor("scan")
  def ls(): Iterator[Option[KVectorLite]] = scan()

  @AliasFor("scan")
  def all(): Iterator[Option[KVectorLite]] = scan()

}

