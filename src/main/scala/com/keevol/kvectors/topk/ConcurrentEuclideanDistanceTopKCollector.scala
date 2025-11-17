package com.keevol.kvectors.topk

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

import java.util.{Comparator, PriorityQueue}

/**
 * A topK collector for concurrently collecting items in multi-thread execution.
 *
 * @param k the number of topK
 */
class ConcurrentEuclideanDistanceTopKCollector(val k: Int) extends TopKCollector[IdWithDistance] {
  require(k > 0, "K must be a positive integer.")

  // reverse comparator for a max-heap (永远不要用减法和类型转换来实现浮点数或长整型的 Comparator。始终使用语言提供的安全的 compare 方法。)
  private val comparator: Comparator[IdWithDistance] = (o1: IdWithDistance, o2: IdWithDistance) => java.lang.Double.compare(o2.distance, o1.distance)

  /**
   * 因为近乎是方法粒度做同步，所以，也就没有必要使用 PriorityBlockingQueue 了，用了反而还多了并发的负担。
   * 主要是 PriorityBlockingQueue 不解决多步骤的并发控制问题。
   */
  private val collector = new PriorityQueue[IdWithDistance](k, comparator)

  override def add(value: IdWithDistance): Unit = {
    collector.synchronized {
      if (collector.size() < k) {
        collector.add(value)
      } else {
        if (value.distance < collector.peek().distance) {
          collector.poll() // remove the top
          collector.add(value)
        }
      }
    }
  }

  override def getTopK: List[IdWithDistance] = collector.synchronized {
    collector.toArray(new Array[IdWithDistance](0)).toList.sortBy(_.distance)
  }
}

