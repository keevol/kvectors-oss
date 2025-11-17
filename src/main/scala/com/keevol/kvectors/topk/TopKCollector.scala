package com.keevol.kvectors.topk

/**
 * top abstraction for topk problem.
 *
 * @tparam T data type in heap
 */
trait TopKCollector[T] {
  def add(value: T): Unit

  def getTopK: List[T]

  /**
   * merge other TopKCollector's topK items into current one
   *
   * @param another TopKCollector
   * @return current instance of TopKCollector for chaining of method call
   */
  def mergeFrom(another: TopKCollector[T]): TopKCollector[T] = {
    another.getTopK.foreach(add)
    this
  }
}
