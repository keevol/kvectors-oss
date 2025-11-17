package com.keevol.kvectors.fs

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

import com.keevol.kvectors.utils.Closables
import org.slf4j.LoggerFactory

import java.io.{File, RandomAccessFile}
import java.util.concurrent.atomic.AtomicInteger


class KRawVectorWriter(dataFile: File, version: Short = 1) extends VectorIO {
  private val logger = LoggerFactory.getLogger(classOf[KRawVectorWriter])

  private val dataWriter = new RandomAccessFile(dataFile, "rw")

  private val dimensionCounter: AtomicInteger = new AtomicInteger()

  def write(id: Long, vector: Array[Float]): Unit = {
    if (dimensionCounter.get() == 0) {
      logger.info("first write, detect vector dimension and mark.")
      // first record, take its dimension as metadata
      dimensionCounter.set(vector.length)
      writeHeader(version, dimensionCounter.get(), dataWriter)
    }
    dataWriter.writeLong(id)
    dataWriter.write(ByteAndBuffer.floatsToBytes(vector))
//    vector.foreach(v => dataWriter.writeFloat(v)) // leave here for comparison
  }

  override def close(): Unit = {
    Closables.closeWithLog(dataWriter)
  }
}