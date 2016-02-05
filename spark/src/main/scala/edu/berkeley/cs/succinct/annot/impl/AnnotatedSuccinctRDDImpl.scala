package edu.berkeley.cs.succinct.annot.impl

import edu.berkeley.cs.succinct.annot.AnnotatedSuccinctRDD
import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.succinct.annot.AnnotatedSuccinctPartition

class AnnotatedSuccinctRDDImpl private[succinct](val partitionsRDD: RDD[AnnotatedSuccinctPartition])
  extends AnnotatedSuccinctRDD(partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  val recordCount: Long = partitionsRDD.map(_.count).aggregate(0L)(_ + _, _ + _)

  /** Set the name for the RDD; By default set to "AnnotatedSuccinctRDD" */
  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }

  setName("AnnotatedSuccinctRDD")

  /**
    * Persists the Succinct partitions at the specified storage level, ignoring any existing target
    * storage level.
    */
  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  /** Cannot un-persist. */
  override def unpersist(blocking: Boolean = true): this.type = {
    this
  }

  /** Persists the Succinct partitions at MEMORY_ONLY level. */
  override def cache(): this.type = {
    partitionsRDD.persist(StorageLevel.MEMORY_ONLY)
    this
  }

  override def count(): Long = {
    recordCount
  }
}
