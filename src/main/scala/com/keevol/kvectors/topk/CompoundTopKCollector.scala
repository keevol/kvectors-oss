package com.keevol.kvectors.topk

/**
 * <pre>
 * :::    ::: :::::::::: :::::::::: :::     :::  ::::::::  :::
 * :+:   :+:  :+:        :+:        :+:     :+: :+:    :+: :+:
 * +:+  +:+   +:+        +:+        +:+     +:+ +:+    +:+ +:+
 * +#++:++    +#++:++#   +#++:++#   +#+     +:+ +#+    +:+ +#+
 * +#+  +#+   +#+        +#+         +#+   +#+  +#+    +#+ +#+
 * #+#   #+#  #+#        #+#          #+#+#+#   #+#    #+# #+#
 * ###    ### ########## ##########     ###      ########  ##########
 * </pre>
 * <p>
 * KEEp eVOLution!
 * <p>
 *
 * @author fq@keevol.cn
 * @since 2017.5.12
 *        <p>
 *        Copyright 2017 © 杭州福强科技有限公司版权所有 (<a href="https://www.keevol.cn">keevol.cn</a>)
 */

import java.util
import java.util.stream.Collectors
import java.util.{Comparator, PriorityQueue}

/**
 * this is mainly for cosine similarity result collect.
 *
 * as to euclidean distance, turn to [[com.keevol.kvectors.topk.EuclideanDistanceTopKCollector]]
 *
 * A new abstraction on TopKCollector problem is defined at [[TopKCollector]],
 * but since a lot of implementations of Vector Collection had use this one for topK collect,
 * we will keep it here which may last for a long time.
 *
 * @param k count of result
 * @param concurrently whether the topKCollector will be accessed by multiple threads.
 */
class CompoundTopKCollector(k: Int, concurrently: Boolean = false) {
  private val idWithScoreComparator = new Comparator[IdWithScore] {
    override def compare(o1: IdWithScore, o2: IdWithScore): Int = java.lang.Float.compare(o1.score, o2.score)
  }
  private val collector = new PriorityQueue[IdWithScore](idWithScoreComparator)

  def add(idWithScore: IdWithScore): Unit = {
    if (concurrently) {
      collector.synchronized {
        doAdd(idWithScore)
      }
    } else {
      doAdd(idWithScore)
    }
  }

  private def doAdd(idWithScore: IdWithScore): Unit = {
    if (collector.size() < k) {
      collector.add(idWithScore)
    } else if (idWithScore.score > collector.peek().score) {
      collector.poll()
      collector.add(idWithScore)
    }
  }

  /**
   * return both id and score for debug
   *
   * @param sorted if needed, default is true
   * @return topK elements
   */
  def getTopK(sorted: Boolean = true): java.util.List[IdWithScore] = {
    if (concurrently) {
      collector.synchronized {
        doGetTopK(sorted)
      }
    } else {
      doGetTopK(sorted)
    }
  }

  private def doGetTopK(sorted: Boolean = true): java.util.List[IdWithScore] = {
    var stream = collector.stream()
    if (sorted) {
      stream = stream.sorted(idWithScoreComparator)
    }
    stream.collect(Collectors.toList[IdWithScore])
  }

  /**
   * without any sort
   */
  def getTopKIds: java.util.List[Long] = {
    if (concurrently) {
      collector.synchronized {
        doGetTopKIds()
      }
    } else {
      doGetTopKIds()
    }
  }

  private def doGetTopKIds(): java.util.List[Long] = {
    val results = new util.ArrayList[Long]()
    val iter = collector.iterator()
    while (iter.hasNext) {
      results.add(iter.next().id)
    }
    results
  }
}


