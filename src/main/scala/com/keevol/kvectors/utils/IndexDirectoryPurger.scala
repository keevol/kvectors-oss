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

import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import java.io.File
import java.util.concurrent.TimeUnit

object IndexDirectoryPurger {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  private def listFileInNumOrder(indexDir: File) = indexDir.listFiles(_.isDirectory).sortBy(_.getName.toLong)

  /**
   * the prerequisite is that the name of index folder is value of [[System.currentTimeMillis()]]
   *
   * @param indexDir to purge with
   * @param maxKept how many to keep
   */
  def purge(indexDir: File, maxKept: Int = 11): Unit = {
    val indexes = listFileInNumOrder(indexDir)
    if (indexes.length > maxKept) {
      (0 until indexes.length - maxKept).foreach(i => {
        logger.info(s"purge legacy index folder: ${indexes(i)} under ${indexDir}")
        FileUtils.forceDelete(indexes(i))
      })
    }
  }

  private def printDir(indexDir: File): Unit = {
    listFileInNumOrder(indexDir).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val indexDir = new File("IndexDirectoryPurgerTestFolder")
    FileUtils.forceMkdir(indexDir)

    try {
      (0 until 4).foreach(_ => {
        FileUtils.forceMkdir(new File(indexDir, System.currentTimeMillis().toString))
        TimeUnit.SECONDS.sleep(1)
      })

      println("before purge: ")
      printDir(indexDir)

      purge(indexDir, maxKept = 3)

      println("after purge: ")
      printDir(indexDir)
    } finally {
      FileUtils.deleteDirectory(indexDir)
    }


  }
}