package com.keevol.kvectors.collections.utils

import com.keevol.kvectors.collections.KVectorCollection
import org.apache.commons.io.FileUtils

import java.io.File

object DropCollectionRoutine {
  def execute(collection: KVectorCollection, dataDir: File): Unit = {
    try {
      collection.close()
    } finally {
      FileUtils.deleteDirectory(dataDir)
    }
  }
}