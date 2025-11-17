package com.keevol.kvectors

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

import com.keevol.goodies.threads.Threads
import com.keevol.kvectors.collections._
import com.keevol.kvectors.collections.creators.KVectorCollectionCreator
import com.keevol.kvectors.enums.{CompressionStrategy, IndexStrategy, SimilarityAlg}
import com.keevol.kvectors.utils.{Closables, Encoding, With}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.{StringUtils, Strings}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import scala.collection.mutable.ArrayBuffer

class KVectors(val dataDir: File = KVectors.DEFAULT_DATA_DIR) extends AutoCloseable {
  private val logger: Logger = LoggerFactory.getLogger(classOf[KVectors])


  private val collections: ConcurrentMap[String, KVectorCollection] = new ConcurrentHashMap[String, KVectorCollection]()

  // do initialization
  Threads.preset()
  require(dataDir != null)
  if (!dataDir.exists()) {
    FileUtils.forceMkdir(dataDir)
  }
  require(dataDir.isDirectory)
  dataDir.listFiles(file => file.isDirectory).foreach(collectionDir => {
    initializeVectorCollection(collectionDir.getName, dataDir, collectionDir)
  })
  // end of initialization

  private def initializeVectorCollection(name: String, kdbRootDataDir: File, collectionDir: File): Unit = {
    logger.info(s"collectionDir: ${collectionDir}")
    val vectorCollection: KVectorCollection = Option(collectionDir).collect(HyperplanesLshKVectorCollection) match {
      case Some(vc) =>
        logger.info(s"collected from specific collection partial function: ${vc.getClass}")
        vc

      case None =>
        val typ = StringUtils.trimToEmpty(FileUtils.readFileToString(new File(getDataDirOfVectorCollection(kdbRootDataDir, name), s"${name}.typ"), Encoding.default()))
        logger.info(s"initialize vector collection: ${name} with type='${typ}'")
        if (Strings.CI.equals(typ, classOf[AnnIndexKVectorCollection].getName)) {
          new AnnIndexKVectorCollection(name, kdbRootDataDir)
        } else if (Strings.CI.equals(typ, classOf[MapdbKVectorCollection].getName)) {
          new MapdbKVectorCollection(name, kdbRootDataDir)
        } else {
          throw new IllegalArgumentException(s"no proper constructor found for typ=${typ} of collection=${name}")
        }
    }
    vectorCollection.reload()
    collections.put(name, vectorCollection)
  }


  def containsCollection(name: String): Boolean = collections.containsKey(name)

  def listCollections(): Array[String] = {
    val arrayBuffer = new ArrayBuffer[String]()
    val iter = collections.keySet().iterator()
    while (iter.hasNext) {
      arrayBuffer.append(iter.next())
    }
    arrayBuffer.toArray
  }

  /**
   * This is the third way which is also the recommended way to create vector collection in kvectors.
   *
   * For advanced users.
   *
   * @param vectorCollectionCreator the creator / factory for creating vector collection instance.
   */
  def createVectorCollection(vectorCollectionCreator: KVectorCollectionCreator): Unit = {
    val vc = vectorCollectionCreator.create()
    vc.reload()
    collections.put(vc.collectionName(), vc)

    vc match {
      case collection: ConfigInspectableVectorCollection =>
        collection.saveCreationConfig()
      case _ => // ignore
    }
  }

  /**
   * This is the initial version which is not so complete and mature(not exactly, in fact, it's a minimal complete version with flat + HNSW index, with these index, 80% requirements can meet)
   *
   * If more index types are needed, then we recommend to use [[KVectors#createVectorCollection(com.keevol.kvectors.collections.creators.KVectorCollectionCreator)]]
   *
   * @param name collection name
   * @param indexStrategy index strategy of vector collection, index or not
   * @param compressionStrategy compress method to use
   * @param similarityAlg which alg to use to compute similarity of vectors
   * @param inMemory memory-only vector collection or not
   */
  def createVectorCollection(name: String,
                             indexStrategy: IndexStrategy = IndexStrategy.NO_INDEX,
                             compressionStrategy: CompressionStrategy = CompressionStrategy.NO,
                             similarityAlg: SimilarityAlg = SimilarityAlg.COSINE,
                             inMemory: Boolean = false): Unit = {
    createVectorCollectionInternal(name, indexStrategy, compressionStrategy, similarityAlg, inMemory) {
      val col = indexStrategy match {
        case IndexStrategy.NO_INDEX => {
          if (inMemory) {
            new TransientKVectorCollection(name)
          } else {
            new MapdbKVectorCollection(name, dataDir)
          }
        }
        case IndexStrategy.ANN => {
          new AnnIndexKVectorCollection(name, dataDir)
        }
      }
      col
    }
  }

  private def createVectorCollectionInternal(name: String,
                                             indexStrategy: IndexStrategy = IndexStrategy.NO_INDEX,
                                             compressionStrategy: CompressionStrategy = CompressionStrategy.NO,
                                             similarityAlg: SimilarityAlg = SimilarityAlg.COSINE,
                                             inMemory: Boolean = false)(collectionInitiator: => KVectorCollection): Unit = {
    if (collections.containsKey(name)) {
      throw new IllegalArgumentException(s"vector collection with name:${name} exists.")
    }

    val col = collectionInitiator
    col.indexStrategy = indexStrategy
    col.compressionStrategy = compressionStrategy
    col.similarityAlg = similarityAlg
    col.reload()

    // save col type
    if (!inMemory) {
      // this behavior can be separated into KVectorCollection abstraction, let's say, saveCreationConfiguration()?
      FileUtils.writeStringToFile(new File(getDataDirOfVectorCollection(dataDir, name), s"${name}.typ"), col.getClass.getName, Encoding.default(), false)
    }

    collections.put(name, col)
  }

  def getCollection(name: String): Option[KVectorCollection] = {
    if (collections.containsKey(name)) Some(collections.get(name)) else None
  }

  /**
   * This is dangerous operation, so take it seriously, we will drop everything and purge the data.
   *
   * @param name the collection name
   */
  def dropCollection(name: String): Unit = {
    if (collections.containsKey(name)) {
      collections.get(name).drop()
      collections.remove(name)
    }
  }

  override def close(): Unit = {
    collections.values.forEach(c => {
      logger.info(s"closing vector collection: ${c.collectionName()}...")
      Closables.closeWithLog(c)
    })
  }

  private def getDataDirOfVectorCollection(kdbDir: File, collectionName: String): File = new File(kdbDir, collectionName)
}


object KVectors {

  val DEFAULT_DATA_DIR = new File(System.getProperty("user.home"), ".kvectors")
  if (!DEFAULT_DATA_DIR.exists()) {
    FileUtils.forceMkdir(DEFAULT_DATA_DIR)
  }

  /**
   * demo code for how to use KVectors as library.
   */
  def main(args: Array[String]): Unit = {
    val vectorDb = new KVectors(new File("somewhere as basedir"))

    With(vectorDb) {
      vectorDb.createVectorCollection("default", IndexStrategy.NO_INDEX)

      vectorDb.getCollection("default").foreach(vectorCollection => {
        //      vectorCollection.add(v)
        //      vectorCollection.topK(v, 10)
      })
    }
  }

}