package edu.berkeley.cs.succinct.kv.impl

import edu.berkeley.cs.succinct.kv.SuccinctStringKVRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.succinct.kv.SuccinctStringKVPartition
import org.apache.spark.{OneToOneDependency, TaskContext}

import scala.reflect.ClassTag

class SuccinctStringKVRDDImpl[K: ClassTag] private[succinct](
                                                              val partitionsRDD: RDD[SuccinctStringKVPartition[K]],
                                                              val firstKeys: Array[K],
                                                              val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
                                                            (implicit ordering: Ordering[K])
  extends SuccinctStringKVRDD[K](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  val recordCount: Long = partitionsRDD.map(_.count).aggregate(0L)(_ + _, _ + _)

  /** Set the name for the RDD; By default set to "SuccinctStringKVRDD" */
  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }

  setName("SuccinctStringKVRDD")

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

  /** Gets the values for the specified keys; adds null values for keys that don't exist. **/
  def multiget(keys: Array[K]): Map[K, String] = {
    val keysByPartition = keys.groupBy(k => partitionIdx(k))
    val partitions = keysByPartition.keys.toSeq
    val results: Array[Array[(K, String)]] = context.runJob(partitionsRDD,
      (context: TaskContext, partIter: Iterator[SuccinctStringKVPartition[K]]) => {
        if (partIter.hasNext && keysByPartition.contains(context.partitionId())) {
          val part = partIter.next()
          part.multiget(keysByPartition(context.partitionId()))
        } else {
          Array.empty
        }
      }, partitions)
    results.flatten.toMap
  }

  /** Get the partition index for a particular key. **/
  def partitionIdx(key: K): Int = findKey(key)

  /** Find the index of a particular key using binary search. **/
  private def findKey(key: K): Int = {
    var (low, high) = (0, firstKeys.length - 1)

    while (low <= high)
      (low + high) / 2 match {
        case mid if ordering.gt(firstKeys(mid), key) => high = mid - 1
        case mid if ordering.lt(firstKeys(mid), key) => low = mid + 1
        case mid => return mid
      }
    -1
  }

  /**
    * Bulk append data to SuccinctStringKVRDD; returns a new SuccinctStringKVRDD, with the newly appended
    * data encoded as Succinct data structures. The original RDD is removed from memory after this
    * operation.
    *
    * @param data                 The data to be appended.
    * @param preservePartitioning Preserves the partitioning for the appended data if true;
    *                             repartitions the data otherwise.
    * @return A new SuccinctStringKVRDD containing the newly appended data.
    */
  def bulkAppend(data: RDD[(K, String)], preservePartitioning: Boolean = false): SuccinctStringKVRDD[K] = {

    val countPerPartition: Double = count().toDouble / partitionsRDD.partitions.length.toDouble
    val nNewPartitions: Int = Math.ceil(data.count() / countPerPartition).toInt

    def partition(data: RDD[(K, String)]): RDD[(K, String)] = {
      if (preservePartitioning) data
      else data.repartition(nNewPartitions)
    }

    val newPartitions = partition(data).sortByKey()
      .mapPartitions(SuccinctStringKVRDD.createSuccinctStringKVPartition[K])
    val newSuccinctRDDPartitions = partitionsRDD.union(newPartitions).cache()
    partitionsRDD.unpersist()
    val firstKeys = newSuccinctRDDPartitions.map(_.firstKey).collect()
    new SuccinctStringKVRDDImpl[K](newSuccinctRDDPartitions, firstKeys)
  }

  /**
    * Count the number of KV-pairs in the SuccinctStringKVRDD.
    *
    * @return The number of KV-pairs in the SuccinctStringKVRDD.
    */
  override def count(): Long = {
    recordCount
  }
}
