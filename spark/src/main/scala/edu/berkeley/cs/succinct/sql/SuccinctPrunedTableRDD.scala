package edu.berkeley.cs.succinct.sql

import edu.berkeley.cs.succinct.SuccinctIndexedFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

class SuccinctPrunedTableRDD(
                              val partitionsRDD: RDD[SuccinctIndexedFile],
                              val succinctSerializer: SuccinctSerializer,
                              val reqColsCheck: Map[String, Boolean],
                              val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends RDD[Row](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  /** Overrides [[RDD]]]'s compute to return a [[SuccinctTableIterator]]. */
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val succinctIterator = firstParent[SuccinctIndexedFile].iterator(split, context)
    if (succinctIterator.hasNext) {
      new SuccinctPrunedTableIterator(succinctIterator.next(),
        succinctSerializer, reqColsCheck)
    } else {
      Iterator[Row]()
    }
  }

  /** Set the name for the RDD; By default set to "SuccinctPrunedTableRDD". */
  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }

  setName("SuccinctPrunedTableRDD")

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

  /**
   * Returns the RDD of partitions.
   *
   * @return The RDD of partitions.
   */
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
   * Returns first parent of the RDD.
   *
   * @return The first parent of the RDD.
   */
  protected[succinct] def getFirstParent: RDD[SuccinctIndexedFile] = {
    firstParent[SuccinctIndexedFile]
  }

}
