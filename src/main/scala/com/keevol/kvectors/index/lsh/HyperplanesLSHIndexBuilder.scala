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

import com.keevol.kvectors.VectorFloats
import com.keevol.kvectors.index.KVectorIndexBuilder
import com.keevol.kvectors.repository.{EmptyVectorSupplier, VectorSupplier}
import com.keevol.kvectors.utils.RandomVector
import io.github.jbellis.jvector.vector.types.VectorFloat


/**
 *
 * hyperplanes LSH index builder
 *
 *
 * = LSH Parameter Tuning Guide (`k` vs. `L`) =
 *
 * The performance and effectiveness of LSH are critically dependent on the trade-off
 * between its two core parameters: `k` and `L`. This guide outlines the principles
 * for tuning them based on different application requirements.
 *
 * == Core Concepts ==
 *
 *   - '''`k` (Hash Length / Number of Random Planes)''': Controls the strictness of a "collision",
 *     which directly impacts '''precision'''. It can be analogized to the '''mesh size of a fishing net'''.
 *     A larger `k` means a smaller mesh, catching only fish that are a perfect size match.
 *
 *   - '''`L` (Number of Hash Tables)''': Controls the probability of finding neighbors,
 *     which directly impacts '''recall'''. It can be analogized to the '''number of fishing nets cast'''
 *     into a lake. A larger `L` means more nets are cast over a wider area, increasing the
 *     chances of catching the target fish.
 *
 * == Tuning Heuristics ==
 *
 * | '''Scenario Requirement'''                                                                 | '''k''' (Hash Length)              | '''L''' (Number of Hash Tables)          | '''Analysis'''                                                                                                                                                                                          |
 * |--------------------------------------------------------------------------------------------|------------------------------------|------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
 * | '''High Recall''' <br> (Prioritize finding all possible matches, even with false positives) <br> ''e.g., Copyright detection, malicious image filtering'' | Small (e.g., `k=16` to `32`)     | Very Large (e.g., `L=256` to `1024+`)   | A small `k` makes the collision condition lenient, ensuring similar vectors are more likely to collide. A large `L` provides ample opportunities for collision, maximizing the chance of finding all potential neighbors. The trade-off is high memory usage and a large candidate set requiring expensive reranking. |
 * | '''High Precision''' <br> (Prioritize confidence in matches, avoiding false positives) <br> ''e.g., Deduplication systems, 1:1 face verification''     | Large (e.g., `k=64` to `256`)    | Small (e.g., `L=64` to `128`)      | A large `k` makes the collision condition extremely strict, ensuring that only highly similar vectors can collide, making the results very reliable. `L` can be smaller as the goal is not to find all neighbors, but only the most dependable matches. |
 * | '''Balanced Approach''' <br> (Most common applications) <br> ''e.g., Recommendation systems, image search'' | Medium (e.g., `k=32` to `64`)    | Medium (e.g., `L=128` to `256`)    | This is the most common configuration. The goal is to find a `k` and `L` combination through experimentation that keeps recall and query latency within an acceptable range, without causing excessive memory consumption. |
 * | '''Extremely Memory Constrained'''                                                         | Large (e.g., `k=64+`)          | Very Small (e.g., `L=16` to `32`)    | This is a last resort when memory is the primary bottleneck. A larger `k` reduces the size of each bucket, while a very small `L` controls the total memory footprint. The significant trade-off is a much lower recall. |
 *
 * @note The optimal parameter combination is highly dependent on the dataset. It is strongly
 *       recommended to determine the best values for `k` and `L` by running offline
 *       experiments on your specific data.
 *
 * @param k random hyperplane number，e.g., `k=32` to `64` for balance scenarios
 * @param L how many hash table to do parallel indexing and retrieval, e.g., `L=128` to `256` for balance scenarios
 * @param dimension vector dimension
 */
class HyperplanesLSHIndexBuilder(name: String, k: Int, L: Int, dimension: Int) extends KVectorIndexBuilder[HyperplanesLSHIndex] {

  require(k % 8 == 0 && k <= 256, "k must be 8/16/32/64/../256")

  def build(): HyperplanesLSHIndex = build(EmptyVectorSupplier())

  /**
   * build index with a name and vector source and return the built index result (file or folder as per specific index types)
   *
   * we hold L HyperplanesHasher which has K hyperplane(random vector)
   *
   * @param vectorSupplier the source vector supplier which will emit a new iterator of vectors.
   * @return result index
   */
  override def build(vectorSupplier: VectorSupplier): HyperplanesLSHIndex = {
    val hashers = new Array[HyperplanesHasher](L)
    for (li <- hashers.indices) {
      hashers.update(li, createHyperplanes(li))
    }
    val index = HyperplanesLSHIndex(name = name, k = k, l = L, dimension = dimension, hashers = hashers)
    index
  }

  /**
   * A hyperplane is just a random gaussian vector
   *
   * we need to create K such vectors.
   *
   * @return count of K of HyperplanesHasher
   */
  private def createHyperplanes(i: Int): HyperplanesHasher = {
    val hyperplanes = new Array[VectorFloat[_]](k)
    for (i <- hyperplanes.indices) {
      val hyperplane = RandomVector(dimension = dimension, normalize = true)
      hyperplanes.update(i, VectorFloats.of(hyperplane))
    }
    new HyperplanesHasher(s"${name}-hasher-${i}", hyperplanes)
  }
}