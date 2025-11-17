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
import java.math.{BigInteger => JBigInt}

case class Point(x: Double, y: Double, label: String)

/**
 * Utils for computing distance in math.
 *
 */
object Distances {

  def euclidean(a: Point, b: Point): Double = {
    math.sqrt(math.pow(a.x - b.x, 2) + math.pow(a.y - b.y, 2))
  }

  def euclidean(a: Array[Double], b: Array[Double]): Double = {
    math.sqrt((a zip b).map { case (x, y) => math.pow(x - y, 2) }.sum)
  }

  /**
   * 高效计算两个64位long类型哈希值之间的汉明距离。
   * 这是pHash场景下的最佳实现方法。
   *
   * @param hash1 第一个64位哈希值
   * @param hash2 第二个64位哈希值
   * @return 汉明距离
   */
  def hammingDistance(hash1: Long, hash2: Long): Int = {
    // 1. 使用异或(XOR)找出所有不同的位，不同的位会变成1
    val xorResult = hash1 ^ hash2;

    // 2. 使用内置的 bitCount 方法计算结果中 '1' 的数量
    // 这就是两个哈希值不同的位的总数，即汉明距离
    return java.lang.Long.bitCount(xorResult);
  }

  /**
   * hamming distance is also used in SimHash computation.
   *
   * This is a implementation with java's BigInteger.
   *
   * @param hash1 hash value in BigInteger type
   * @param hash2 another hash value in BigInteger type
   */
  def hammingDistanceBetween(hash1: JBigInt, hash2: JBigInt): Unit = hash1.xor(hash2).bitCount()
}