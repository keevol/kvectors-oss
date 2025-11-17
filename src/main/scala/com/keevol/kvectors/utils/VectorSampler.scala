package com.keevol.kvectors.utils

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

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * result from copilot with gemini
 *
 * A dynamic demonstration of how reservoir sampling work can be found at: [[https://afoo.me/animates/reservoir-sampling.html]]
 */
object VectorSampler {

  /**
   * 水塘抽样
   *
   * @param sourceIterator 流式向量数据输入， 通常读取文件
   * @param sampleCount 水塘大小， 也就是抽样的数量
   * @return 返回最终抽样结果
   */
  def reservoirSample(sourceIterator: Iterator[Array[Float]], sampleCount: Int): Array[Array[Float]] = {
    // 1. 創建水塘並用前 k 個元素填滿
    val reservoir = new ArrayBuffer[Array[Float]](sampleCount)
    var elementsSeen = 0

    // 先填滿水塘
    while (sourceIterator.hasNext && elementsSeen < sampleCount) {
      reservoir.append(sourceIterator.next())
      elementsSeen += 1
    }

    // 如果數據源元素不足 k 個，直接返回
    if (elementsSeen < sampleCount) {
      return reservoir.toArray
    }

    // 2. 遍歷數據流的剩餘部分
    val random = new Random()
    while (sourceIterator.hasNext) {
      val currentVector = sourceIterator.next()
      elementsSeen += 1

      // 3. 決定是否替換水塘中的元素
      val j = random.nextInt(elementsSeen) // 生成 [0, elementsSeen - 1] 之間的隨機數
      if (j < sampleCount) {
        reservoir(j) = currentVector
      }
    }

    reservoir.toArray
  }

}