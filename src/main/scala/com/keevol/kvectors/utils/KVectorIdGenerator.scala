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

import com.keevol.kvectors.lifecycles.Lifecycle
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong

class KVectorIdGenerator(dataDir: File) extends Lifecycle {
  private val idGeneratorSnLFile = new File(dataDir, "id.gen")

  private val idGenerator = new AtomicLong()

  def next(): Long = idGenerator.getAndIncrement()

  def current(): Long = idGenerator.get()

  override protected def doStart(): Unit = {
    if (idGeneratorSnLFile.exists()) {
      val str = StringUtils.trimToEmpty(FileUtils.readFileToString(idGeneratorSnLFile, StandardCharsets.UTF_8))
      if (StringUtils.isNotEmpty(str)) {
        idGenerator.set(str.toLong)
      }
    }
  }

  override protected def doStop(): Unit = {
    FileUtils.writeStringToFile(idGeneratorSnLFile, idGenerator.get().toString, StandardCharsets.UTF_8, false)
  }
}