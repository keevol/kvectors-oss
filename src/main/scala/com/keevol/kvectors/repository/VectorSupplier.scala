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
import com.keevol.kvectors.KVectorLite

/**
 * A Vector Supplier supplies a way to provide streaming and lazy way to consume vectors.
 */
trait VectorSupplier {
  /**
   * must return a new Iterator at each call, since the iterator will be consumed only once at each time.
   *
   * NOTE: as an implementation contract of Iterator, you SHOULD NOT change state in hasNext method which should stay readonly(immutable). DO CHANGE state in next() method.
   */
  def iterator(): Iterator[KVectorLite]
}

/**
 * for index type like LSH, no initial vector set needed to build an index.
 */
class EmptyVectorSupplier extends VectorSupplier {

  /**
   * must return a new Iterator at each call, since the iterator will be consumed only once at each time.
   */
  override def iterator(): Iterator[KVectorLite] = new Iterator[KVectorLite] {
    override def hasNext: Boolean = false

    override def next(): KVectorLite = null
  }
}

object EmptyVectorSupplier {
  private val instance = new EmptyVectorSupplier

  def apply(): EmptyVectorSupplier = instance
}

/**
 * a vector supplier only offer one data entry.
 *
 * @param v single entry of vector
 */
class SingleVectorSupplier(v: KVectorLite) extends VectorSupplier {

  /**
   * must return a new Iterator at each call, since the iterator will be consumed only once at each time.
   */
  override def iterator(): Iterator[KVectorLite] = new Iterator[KVectorLite] {
    var count = 0

    override def hasNext: Boolean = count == 0

    override def next(): KVectorLite = try {
      v
    } finally {
      count = -1
    }
  }
}

object SingleVectorSupplier {
  def main(args: Array[String]): Unit = {
    val supplier = new SingleVectorSupplier(KVectorLite(1L, Array(0)))
    val iter = supplier.iterator()
    while (iter.hasNext) {
      val kv = iter.next()
      println(s"id of kv = ${kv.id}")
    }
  }
}
