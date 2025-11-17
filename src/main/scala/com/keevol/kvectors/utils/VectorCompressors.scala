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
object VectorCompressors {
  /**
   * <quote>随机投影的数学保证（JL 引理）是建立在一个前提之上的：所有的向量都必须被投影到同一个随机子空间中。</quote>
   *
   * It's deemed to use projector callback to project/compress multiple vectors, single vector projection is meaningless.
   *
   * @param originalDim the vector dim to be compressed with random projection
   * @param targetDimension the projection dimension which should smaller than vector's
   * @return result as per projector handler callback
   */
  def randomProjection[T](originalDim: Int = 1024, targetDimension: Int = 128)(projector: RandomProjection => T): T = {
    require(originalDim > targetDimension, s"originalDim must have a length longer than target dimension to compress to, currently (${originalDim}) <= ${targetDimension}")
    val compressor = new RandomProjection(originalDim, targetDimension)
    projector.apply(compressor)
  }


}