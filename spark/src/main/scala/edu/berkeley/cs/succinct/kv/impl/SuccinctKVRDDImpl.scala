package edu.berkeley.cs.succinct.kv.impl

import edu.berkeley.cs.succinct.kv.{SuccinctKVPartition, SuccinctKVRDD}
import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class SuccinctKVRDDImpl[K: ClassTag] private[succinct](
    val partitionsRDD: RDD[SuccinctKVPartition[K]],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    (implicit ordering: Ordering[K])
  extends SuccinctKVRDD[K](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  val recordCount: Long = partitionsRDD.map(_.count).aggregate(0L)(_ + _, _ + _)

  /** Set the name for the RDD; By default set to "SuccinctKVRDD" */
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

  /**
    * Count the number of KV-pairs in the SuccinctKVRDD.
    *
    * @return The number of KV-pairs in the SuccinctKVRDD.
    */
  override def count(): Long = {
    recordCount
  }

  /**
    * Bulk append data to SuccinctKVRDD; returns a new SuccinctKVRDD, with the newly appended
    * data encoded as Succinct data structures. The original RDD is removed from memory after this
    * operation.
    *
    * @param data The data to be appended.
    * @param preservePartitioning Preserves the partitioning for the appended data if true;
    *                             repartitions the data otherwise.
    * @return A new SuccinctKVRDD containing the newly appended data.
    */
  def bulkAppend(data: RDD[(K, Array[Byte])], preservePartitioning: Boolean = false):
      SuccinctKVRDD[K] = {

    val countPerPartition: Double = count().toDouble / partitionsRDD.partitions.length.toDouble
    val nNewPartitions: Int = Math.ceil(data.count() / countPerPartition).toInt

    def partition(data: RDD[(K, Array[Byte])]): RDD[(K, Array[Byte])] = {
      if (preservePartitioning) data
      else data.repartition(nNewPartitions)
    }

    val newPartitions = partition(data).sortByKey()
      .mapPartitions(SuccinctKVRDD.createSuccinctKVPartition[K])
    val newSuccinctRDDPartitions = partitionsRDD.union(newPartitions).cache()
    partitionsRDD.unpersist()
    new SuccinctKVRDDImpl[K](newSuccinctRDDPartitions)
  }
}
