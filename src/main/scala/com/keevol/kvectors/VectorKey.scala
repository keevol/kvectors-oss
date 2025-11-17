package com.keevol.kvectors

/**
 * Array of float can't be the key of hash map directly, since no matter java or scala, they will only compare with the reference of the array, not the content of it.
 *
 * So we have to wrap the array of float to redefine the equals and hashCode methods to make the hash map work.
 *
 * @param vector the array of float which will be the content of the key(not itself as the key) in hash map.
 */
case class VectorKey(vector: Array[Float]) {
  override def equals(obj: Any): Boolean = obj match {
    case that: VectorKey => java.util.Arrays.equals(this.vector, that.vector)
    case _ => false
  }

  override def hashCode(): Int = java.util.Arrays.hashCode(vector)
}
