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

import net.jpountz.xxhash.XXHashFactory

import java.nio.charset.StandardCharsets

/**
 * utils for routing hash, xxhash or murmur3 are both good candidate for such scenario.
 *
 * for murmur3, 32 bit is fast and good for hashmap, routing things, which can accept collision rate. If identity or strict hash needed, murmur3 128 is slower(than 32 bit) but a good candidate.
 *
 * Currently, xxhash is the fastest.
 */
object Hashing {

  private val xxhashFactory = XXHashFactory.fastestJavaInstance() // heavy-resource
  private val xxhash64 = xxhashFactory.hash64() // thread-safe
  private val seed = 0L // must be same, so just keep it as 0 (or any value you like)

  private val xxhashFun: HashFunction = new XXHashFunction()

  /**
   * default hashing with xxhash.
   *
   * @param input content to hash with
   * @return hash value as long
   */
  def hash(input: String): Long = xxhash(input)

  def xxhash(input: String): Long = {
    val bytes = input.getBytes(StandardCharsets.UTF_8)
    xxhashBytes(bytes)
  }

  def xxhashBytes(bytes: Array[Byte]): Long = {
    xxhash64.hash(bytes, 0, bytes.length, seed)
  }

  // as to murmur3, google guava has a hash function for it.

  /**
   * Factory method to xxhash function.
   * @return a singleton instance of XXHashFunction
   */
  def xxHashFunction(): HashFunction = xxhashFun
}