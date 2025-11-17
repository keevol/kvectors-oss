package com.keevol.kvectors.fs

import java.io.File
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

/**
 * make them a pair to refer to.
 *
 * @deprecated turn to MemorySegment based impl. from FFM API
 * @param fc the file channel that create mapping from
 * @param mappedBuffer the buffer mapped
 */
@Deprecated
case class Mmap(val fc: FileChannel, val mappedBuffer: MappedByteBuffer, val sourceFile: File)
