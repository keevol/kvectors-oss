package com.keevol.kvectors

/**
 * this is mostly for index building which needs vector value most.
 *
 * usually return as iterator of this type in vector collection.
 *
 * @param id vector id to relate to original vector and its metadata
 * @param value vector value.
 */
case class KVectorLite(id: Long, value: Array[Float])

/**
 * when get specific vector from vector store.
 *
 * @param id vector id, which is enriched after adding to vector collection
 * @param vector the value
 * @param rid relation id to relate to other source
 * @param meta extra info, don't care whether it's in json format or not.
 */
case class KVector(val id: Long, val vector: Array[Float], rid: String, meta: String)

