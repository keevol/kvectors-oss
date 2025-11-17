package com.keevol.kvectors.repository

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
 * <p>
 * Copyright 2017 © 杭州福强科技有限公司版权所有 (<a href="https://www.keevol.cn">keevol.cn</a>)
 */

import com.keevol.kvectors.fs.ByteAndBuffer
import com.keevol.kvectors.lifecycles.Lifecycle
import com.keevol.kvectors.utils.{Encoding, TestVectorGenerator}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption._
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.beans.BeanProperty


class RolloverFileRawVectorRepository(dataDir: File, vectorCollectionName: String) extends Lifecycle {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  require(StringUtils.isNotEmpty(vectorCollectionName))

  @BeanProperty
  var vectorJournalFileSuffixName = ".vec"

  @BeanProperty
  var segmentSize: Long = 1 << 30; // 1G as default which takes mmap limit into consideration.

  private val activeFile = new AtomicReference[String]() // for runtime ref
  private val activeFileMetaFile = new File(dataDir, "active.vec.journal") // for save and load at start and stop
  private val activeChannel: AtomicReference[FileChannel] = new AtomicReference[FileChannel]() // for rollover replacement

  private val vectorFrameBuffer: AtomicReference[ByteBuffer] = new AtomicReference[ByteBuffer]()
  private val dimensionHolder: AtomicInteger = new AtomicInteger()
  // async and conforms to Single-writer principle
  private val rv: LinkedBlockingQueue[(Long, Array[Float])] = new LinkedBlockingQueue[(Long, Array[Float])]()


  override def doStart(): Unit = {
    if (!dataDir.exists()) {
      FileUtils.forceMkdir(dataDir)
    }
    val activeFileName = if (activeFileMetaFile.exists()) {
      FileUtils.readFileToString(activeFileMetaFile, Encoding.default())
    } else {
      nextJournalFilename()
    }
    activeFile.set(activeFileName)

    openActiveFileChannel()

    Thread.ofVirtual().name("RawVectorRolloverWriter thread").uncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = logger.error(s"uncaught exception in ${t.getName}: ${ExceptionUtils.getStackTrace(e)}")
    }).start(() => {
      // asynchronously write in one thread as per single-writer principle
      while (running.get() || (!rv.isEmpty)) {
        val e = rv.poll()
        if (e != null) { // ManyToOneConcurrentLinkedQueue doesn't conform to convention, so the poll may return null as result.
          val (id, vector) = e
          val dimension = vector.length
          val vectorFrameLen = java.lang.Long.BYTES + java.lang.Float.BYTES * dimension // Long(8 bytes) + float(4 bytes) * vector dimension
          if (vectorFrameBuffer.get() == null) {
            val directBuffer = ByteBuffer.allocateDirect(vectorFrameLen)
            vectorFrameBuffer.set(directBuffer)
          }
          if (dimensionHolder.get() == 0) {
            dimensionHolder.set(dimension)
          }
          require(vector.length == dimensionHolder.get(), s"vector dimension collision, expected:${dimensionHolder.get()}, get: ${vector.length}")
          logger.debug(s"write vector with id=${id} to journal...")
          val buffer = vectorFrameBuffer.get()
          buffer.clear()
          buffer.putLong(id)
          buffer.put(ByteAndBuffer.floatsToBytes(vector))
          buffer.flip()
          while (buffer.hasRemaining) {
            activeChannel.get().write(buffer)
          }
          // rollover if necessary
          if (activeChannel.get().size() > segmentSize) {
            // do rollover
            activeChannel.get().close()
            val nextJnlFilename = nextJournalFilename()
            activeFile.set(nextJnlFilename)
            logger.info(s"rollover vector storage file to ${nextJnlFilename}")
            openActiveFileChannel()
          }
        }
      }
    })
  }


  def append(id: Long, vector: Array[Float]): Unit = rv.put((id, vector))

  override def doStop(): Unit = {
    if (vectorFrameBuffer.get() != null) {
      // as to direct buffer, we can do nothing except waiting for gc.
      vectorFrameBuffer.get().clear()
      vectorFrameBuffer.set(null)
    }
    /**
     * another way is to list file as per timestamp without save such meta file.
     */
    val activeFilename = activeFile.get()
    if (StringUtils.isNotEmpty(activeFilename)) {
      FileUtils.writeStringToFile(activeFileMetaFile, activeFilename, Encoding.default(), false)
    }

    if (activeChannel.get() != null) {
      activeChannel.get().close()
      activeChannel.set(null)
    }
  }

  // create new file if non-exists, in our case, after rename or move old active file.
  private def openActiveFileChannel(): Unit = {
    val activeFilename = activeFile.get()
    require(StringUtils.isNotEmpty(activeFilename))
    activeChannel.set(FileChannel.open(new File(dataDir, activeFilename).toPath, CREATE, READ, WRITE))
  }

  private def nextJournalFilename(): String = vectorCollectionName + vectorJournalFileSuffixName + "." + System.currentTimeMillis()

}

object RolloverFileRawVectorRepository {
  def main(args: Array[String]): Unit = {
    val dataDir = new File(".", "test_rollover_repo")
    if (dataDir.exists()) {
      FileUtils.deleteDirectory(dataDir)
    }
    val colName = "test"
    val repo = new RolloverFileRawVectorRepository(dataDir, colName)
    repo.setSegmentSize(8096000)
    repo.start()

    for (i <- 0 until 10000) {
      val floats = TestVectorGenerator.generateVectorOf()
      //        println(s"insert vector with id:${i}")
      repo.append(i, floats)
    }

    println("press any key to exit")
    System.in.read()
    repo.close()
  }
}