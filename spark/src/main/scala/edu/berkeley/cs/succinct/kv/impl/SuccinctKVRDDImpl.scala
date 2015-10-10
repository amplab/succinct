package edu.berkeley.cs.succinct.kv.impl

import edu.berkeley.cs.succinct.SuccinctKV
import edu.berkeley.cs.succinct.kv.SuccinctKVRDD
import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class SuccinctKVRDDImpl[K<: Comparable[K]: ClassTag] private[succinct](
    val partitionsRDD: RDD[SuccinctKV[K]],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends SuccinctKVRDD[K](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  /** Set the name for the RDD; By default set to "SuccinctRDD" */
  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }

  setName("SuccinctKVRDD")

  /**
   * Persists the Succinct partitions at the specified storage level, ignoring any existing target
   * storage level.
   */
  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  /** Un-persists the Succinct partitions using the specified blocking mode. */
  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  /** Persists the Succinct partitions at `targetStorageLevel`, which defaults to MEMORY_ONLY. */
  override def cache(): this.type = {
    partitionsRDD.persist(targetStorageLevel)
    this
  }
}
