package com.keevol.kvectors.utils

import org.apache.commons.io.FileUtils
import org.mapdb.{DB, DBMaker}

import java.io.File

object MapDB {
  /**
   * creator for mapdb with file.
   *
   * @param dbFile file(not directory) for mapdb
   * @param transactionEnable if transactionEnable, commit is a must at each put
   * @return
   */
  def apply(dbFile: File, transactionEnable: Boolean = false): DB = {
    require(dbFile != null)

    if (!dbFile.getParentFile.exists()) {
      FileUtils.forceMkdir(dbFile.getParentFile)
    }

    val maker = DBMaker.fileDB(dbFile).fileMmapEnable().cleanerHackEnable().fileLockDisable().checksumHeaderBypass()
    if (transactionEnable) {
      maker.transactionEnable()
    }
    maker.make()
  }
}