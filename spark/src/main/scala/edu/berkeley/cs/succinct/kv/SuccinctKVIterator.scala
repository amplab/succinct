package edu.berkeley.cs.succinct.kv

import edu.berkeley.cs.succinct.SuccinctKV

/**
 * Iterator for a SuccinctRDD partition.
 *
 * @constructor Create a new SuccinctKVIterator from the underlying SuccinctKV.
 * @param sKV The underlying SuccinctKV.
 */
class SuccinctKVIterator[K<: Comparable[K]] private[succinct](sKV: SuccinctKV[K]) extends Iterator[(K, Array[Byte])] {

  val sKeyIterator = sKV.keyIterator()

  /**
   * Returns true if there are more KV-pairs to iterate over.
   *
   * @return true if there are more KV-pairs to iterate over;
   *         false otherwise.
   */
  override def hasNext: Boolean = sKeyIterator.hasNext

  /**
   * Returns the next KV-pair.
   *
   * @return The next KV-pair.
   */
  override def next(): (K, Array[Byte]) = {
    val key = sKeyIterator.next()
    (key, sKV.get(key))
  }
}
