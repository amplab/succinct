package edu.berkeley.cs.succinct.json.impl

import edu.berkeley.cs.succinct.json.{SuccinctJsonPartition, SuccinctJsonRDD}
import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class SuccinctJsonRDDImpl private[succinct](val partitionsRDD: RDD[SuccinctJsonPartition],
                                            val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends SuccinctJsonRDD(partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  /** Set the name for the RDD; By default set to "SuccinctJsonRDD" */
  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }

  setName("SuccinctJsonRDD")

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
