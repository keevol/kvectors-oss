package com.keevol.kvectors.repository

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

import com.keevol.kvectors.KVectorLite

import java.io.File
import java.lang.foreign.{Arena, MemorySegment}
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util
import java.util.concurrent.ConcurrentHashMap

/**
 * A wrapper swap unit definition to manage all states in one place
 *
 * although the update via writerMemorySegment will be reflected to readerMemorySegment and may cause incomplete data state,
 * but since we have concurrent in-memory structure upfront as barrier,
 * so no concurrent access problem with vector data access.
 *
 * @param journalFile vector journal file in fixed length after write rollover
 * @param arena shared arena only close on vector collection.
 * @param fc file channel for mmap
 * @param readerMemorySegment mmap-ped memory segment from rfc
 * @param writerMemorySegment mmap-ped memory segment from wfc
 * @param lastId the last vector id stored in this journalFile
 */
case class JournalStates(journalFile: File,
                         arena: Arena,
                         fc: FileChannel,
                         readerMemorySegment: MemorySegment,
                         writerMemorySegment: MemorySegment,
                         lastId: Long) {
}

object JournalStates {

  def apply(journalFile: File, lastId: Long): JournalStates = {
    val arena = Arena.ofShared()
    val fc = FileChannel.open(journalFile.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE)
    val readerMemorySegment = fc.map(MapMode.READ_ONLY, 0, journalFile.length(), arena)
    val writerMemorySegment = fc.map(MapMode.READ_WRITE, 0, journalFile.length(), arena)
    JournalStates(journalFile, arena, fc, readerMemorySegment, writerMemorySegment, lastId)
  }

}

/**
 * 如果loadAllVectorsToMemory=true，那activeWriterBuffer基本上可以忽略， activeWriterBuffer主要是为基于mmap读取文件的情况下准备的数据断层补充。
 *
 * updateCache will contain all updates after each bootstrap before shutdown of the vector store,
 * so if high-frequent updates are involved, it seems as if we have memory leak problem, but it's not, because high-frequent updates are not expected in our scenarios,
 * if high-frequent updates are really needed, replay from the single source of truth is another way.
 *
 * @param journals sorted journal resource ref
 * @param activeJournalFile current writing journal file
 * @param activeJournalFileChannel file channel of current writing journal file
 * @param activeWriteBuffer a cache for vectors in current writing journal for read
 * @param activeUpdateWalFile for update op, write them to wal for later IO before rollover
 * @param updateCache all updates should be accumulated here for read if loadAllInMemory option is not enable on Vector Store
 * @param version cas mark for readers, especially when iterating on all vectors in the vector store
 */
case class KVectorStoreActiveStates(journals: util.TreeMap[Long, JournalStates],
                                    activeJournalFile: File,
                                    activeJournalFileChannel: FileChannel,
                                    activeWriteBuffer: ConcurrentHashMap[Long, KVectorLite],
                                    activeUpdateWalFile: File,
                                    activeUpdateWalFileChannel: FileChannel,
                                    updateCache: ConcurrentHashMap[Long, KVectorLite],
                                    version: Long = 0)




