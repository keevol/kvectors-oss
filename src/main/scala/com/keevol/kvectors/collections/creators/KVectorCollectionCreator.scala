package com.keevol.kvectors.collections.creators

import com.keevol.kvectors.collections.KVectorCollection
import com.keevol.kvectors.collections.loader.KVectorCollectionLoader

//case class KVectorCollectionCreation(vc: KVectorCollection, serializedCfg: String)

/**
 * factory of KVectorCollection
 *
 * This is only used when creating a KVectorCollection at first.
 *
 * As to (re)load and initialization again, turn to [[KVectorCollectionLoader]]
 */
trait KVectorCollectionCreator {
  def create(): KVectorCollection

//  /**
//   * handler to convert configuration of vc to string. For scala DSL impl. it's the impl. code of create() method.
//   * @return
//   */
//  def serializedConfig(): String
  //
  //  /**
  //   * although we can inline this to KVectorCollection abstraction,
  //   * but I would like to externalize it here so that it aligns with the creation code of the KVectorCollection
  //   *
  //   * @param cfg KVectorCollection configuration content in string. Different KVectorCollection can have different serialization strategy.
  //   */
  //  def saveConfig(cfg: String, collectionDir: File): Unit

//  /**
//   * save configuration of the vector collection at first after create().
//   *
//   * @param collectionDir where the vc locates at local file system.
//   */
//  def save(collectionDir: File): Unit
}
