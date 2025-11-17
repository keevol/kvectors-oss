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
import com.keevol.kvectors.index.KVectorIndexStore
import com.keevol.kvectors.utils.TypeAlias.Dir
import com.keevol.kvectors.utils.With
import io.github.jbellis.jvector.vector.types.VectorFloat
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.commons.lang3.{StringUtils, Strings}

import java.io.{File, RandomAccessFile}
import java.math.BigInteger
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * save and load specific LSH index to its own dir under root index dir
 *
 * it's deemed to be used by creating at use and dropping after use.
 *
 * TODO: Merge index files to avoid lots of small files
 *
 * @param individualIndexDir this is the dir for specific index (sub dir of the root index dir)
 */
class HyperplanesLSHIndexStore(individualIndexDir: Dir) extends KVectorIndexStore[HyperplanesLSHIndex] {
  private val seqSeparator = "-_-"
  private val hasherFileSuffix = ".ohp" // original hyperplanes
  private val hashtableFileSuffix = ".lsh.bucket" // hash entries with id list
  private val idxFileSuffix = ".lsh.idx"


  override def save(idx: HyperplanesLSHIndex): Unit = {
    val idxFile = getIdxFile(idx.getName)
    val idxFileWriter = new RandomAccessFile(idxFile, "rw")
    With(idxFileWriter) {
      //      idxFileWriter.writeUTF(idx.getName)
      idxFileWriter.writeInt(idx.k)
      idxFileWriter.writeInt(idx.l)
      idxFileWriter.writeInt(idx.dimension)
    }

    for (i <- idx.hashers.indices) {
      val hasher = idx.hashers(i)
      val hasherFile = getHasherFile(hasher.name, i)
      val hasherFileWriter = new RandomAccessFile(hasherFile, "rw")
      With(hasherFileWriter) {
        val initialsVectors = hasher.initialHyperplanes
        hasherFileWriter.writeInt(initialsVectors.length)
        hasherFileWriter.writeInt(idx.dimension)
        initialsVectors.foreach(vf => {
          for (vfi <- 0 until vf.length()) {
            hasherFileWriter.writeFloat(vf.get(vfi))
          }
        })
      }

      val hashtable = idx.lsh(i)
      val hashtableFile = getHashtableFile(i)
      val hashtableWriter = new RandomAccessFile(hashtableFile, "rw")
      With(hashtableWriter) {
        val ks = hashtable.keySet()
        val count = ks.size() // write at last
        //        val keyBytes = idx.k / 8 // we have `require` check on index creation
        val keyBytes = if (idx.k > 64) (idx.k / 8) + 1 else idx.k / 8 // when k > 64 we have to deal with big integer's signum issue
        val keyIterator = ks.iterator()
        while (keyIterator.hasNext) {
          val k = keyIterator.next()
          // write lsh key
          keyBytes match {
            case 1 => hashtableWriter.writeByte(k.asInstanceOf[ByteLSHKey].value)
            case 2 => hashtableWriter.writeShort(k.asInstanceOf[ShortLSHKey].value)
            case 4 => hashtableWriter.writeInt(k.asInstanceOf[IntLSHKey].value)
            case 8 => hashtableWriter.writeLong(k.asInstanceOf[LongLSHKey].value)
            case _ =>
              val ba = k.asInstanceOf[BigIntLSHKey].value.toByteArray
              // 創建一個固定長度為 keyBytes 的緩衝區
              val fixedBa = new Array[Byte](keyBytes)

              // 將 ba 的內容右對齊地複製到 fixedBa 中
              // 這樣可以正確處理符號位，多餘的前導位會被自動填充為 0
              System.arraycopy(ba, 0, fixedBa, fixedBa.length - ba.length, ba.length)

              hashtableWriter.write(fixedBa)
          }
          // write id list
          val ids = hashtable.get(k).keySet()
          hashtableWriter.writeInt(ids.size())
          ids.forEach(id => hashtableWriter.writeLong(id))
        }
        // write file foot for read on load.
        hashtableWriter.writeInt(keyBytes)
        hashtableWriter.writeInt(count)
      }
    }
  }

  override def load(): HyperplanesLSHIndex = {
    val files = FileUtils.listFiles(individualIndexDir, Array(hasherFileSuffix, hashtableFileSuffix, idxFileSuffix), false).asScala
    val idxFileOptions = files.filter(f => Strings.CI.endsWith(f.getName, idxFileSuffix))
    if (idxFileOptions.isEmpty) {
      throw new IllegalStateException("can't find LSH index file entry.")
    }
    val idxFile = idxFileOptions.head
    val idxName = StringUtils.substringBeforeLast(idxFile.getName, idxFileSuffix)
    val idxFileReader = new RandomAccessFile(idxFile, "r")
    With(idxFileReader) {
      val k = idxFileReader.readInt()
      val L = idxFileReader.readInt()
      val dim = idxFileReader.readInt()

      // load hasher and initial vectors
      val hasherFiles = files.filter(f => Strings.CI.endsWith(f.getName, hasherFileSuffix))
      require(hasherFiles.size == L)
      val hashers = new Array[HyperplanesHasher](L)
      hasherFiles.foreach(f => {
        val seq = StringUtils.substringBefore(f.getName, seqSeparator).toInt
        val name = StringUtils.substringBetween(f.getName, seqSeparator, hasherFileSuffix)
        val hasherFileRaf = new RandomAccessFile(f, "r")
        With(hasherFileRaf) {
          val vectorNum = hasherFileRaf.readInt()
          val dimension = hasherFileRaf.readInt()
          require(dim == dimension)
          val initialVectors = new Array[VectorFloat[_]](vectorNum)
          for (vi <- initialVectors.indices) {
            val vector = new Array[Float](dimension)
            for (di <- vector.indices) {
              vector.update(di, hasherFileRaf.readFloat())
            }
            initialVectors.update(vi, VectorFloats.of(vector))
          }
          hashers.update(seq, new HyperplanesHasher(name, initialVectors))
        }
      })

      val index = new HyperplanesLSHIndex(name = idxName, k = k, l = L, dimension = dim, hashers = hashers)
      // load bucket data
      val bucketFiles = files.filter(f => Strings.CI.endsWith(f.getName, hashtableFileSuffix))
      require(bucketFiles.size == L)
      bucketFiles.foreach(f => {
        val seq = StringUtils.substringBefore(f.getName, hashtableFileSuffix).toInt
        val bucketRaf = new RandomAccessFile(f, "r")
        With(bucketRaf) {
          if (bucketRaf.length() <= 8) {
            throw new IllegalStateException("invalid LSH bucket file")
          }

          bucketRaf.seek(bucketRaf.length() - 8)
          val keyBytes = bucketRaf.readInt()
          val entryCount = bucketRaf.readInt()
          bucketRaf.seek(0) // reset position to start read
          for (i <- (0 until entryCount)) {
            // read entry key
            val lshKey = keyBytes match {
              case 1 => ByteLSHKey(bucketRaf.readByte())
              case 2 => ShortLSHKey(bucketRaf.readShort())
              case 4 => IntLSHKey(bucketRaf.readInt())
              case 8 => LongLSHKey(bucketRaf.readLong())
              case _ =>
                val value = new Array[Byte](keyBytes)
                bucketRaf.readFully(value)
                BigIntLSHKey(new BigInteger(value))
            }
            // read entry list and fill the holder
            val totalIdNum = bucketRaf.readInt()
            (0 until totalIdNum).foreach(_ => {
              val id = bucketRaf.readLong()
              index.lsh(seq).computeIfAbsent(lshKey, _ => new ConcurrentHashMap[Long, Boolean]()).put(id, true)
            })
          }

        }

      })

      return index
    }

  }

  private def getIdxFile(name: String): File = new File(individualIndexDir, name + idxFileSuffix)

  private def getHasherFile(basename: String, seq: Int): File = new File(individualIndexDir, s"${seq}${seqSeparator}${basename}${hasherFileSuffix}")

  private def getHashtableFile(seq: Int): File = new File(individualIndexDir, s"${seq}${hashtableFileSuffix}")
}