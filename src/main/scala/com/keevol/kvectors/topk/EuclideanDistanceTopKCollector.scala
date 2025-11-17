package com.keevol.kvectors.topk

import scala.collection.mutable

/**
 * 一个高效的 Top-K 收集器，用于查找欧几里得距离最小的 K 个结果。
 * [Scala 版本] 内部使用一个大小为 K 的最大堆（Max-Heap）实现。
 *
 *
 * NOTE: This is not a thread-safe version, for concurrency access, turn to [[ConcurrentEuclideanDistanceTopKCollector]]
 *
 * @param k 要保留的最近邻的数量。
 */
class EuclideanDistanceTopKCollector(k: Int) {
  require(k > 0, "K must be a positive integer.")

  // 1. 定义一个 Ordering，让 PriorityQueue 按 distance 升序排列。
  //    因为 Scala 的 PriorityQueue 默认是最大堆，所以提供一个自然的升序 Ordering 即可。
  implicit private val maxHeapOrdering: Ordering[IdWithDistance] = Ordering.by(_.distance)

  // 2. 创建 Scala 的 PriorityQueue。它会自动使用上面的隐式 Ordering。
  private val collector = new mutable.PriorityQueue[IdWithDistance]()

  /**
   * 采集一个新的距离结果。
   * 这是最优的 O(log K) "擂台赛" 策略。
   */
  def add(newItem: IdWithDistance): Unit = {
    if (collector.size < k) {
      collector.enqueue(newItem)
    } else {
      // .head 查看堆顶（最大值）
      if (newItem.distance < collector.head.distance) {
        collector.dequeue()
        collector.enqueue(newItem)
      }
    }
  }

  /**
   * 获取最终的 Top-K 结果。
   * @return 一个按距离升序排列的列表。
   */
  def getTopK: List[IdWithDistance] = {
    // PriorityQueue 内部无序（除了堆顶），所以需要排序后返回
    collector.toList.sortBy(_.distance)
  }

  def size: Int = collector.size
}