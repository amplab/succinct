package edu.berkeley.cs.succinct.impl

import edu.berkeley.cs.succinct.{SuccinctIndexedFile, SuccinctRDD}
import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Implementation of SuccinctRDD.
 *
 * @constructor Create a new SuccinctRDDImpl from an RDD of partitions (SuccinctIndexedBuffer) and the target
 *              storage level.
 * @param partitionsRDD The input RDD of partitions.
 * @param targetStorageLevel The storage level for the RDD.
 */
class SuccinctRDDImpl private[succinct](
    val partitionsRDD: RDD[SuccinctIndexedFile],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends SuccinctRDD(partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  val partitionOffsetRanges = partitionsRDD.map(_.getFileRange).collect.sorted
  val partitionRecordIdRanges = partitionsRDD.map(_.getRecordIdRange).collect.sorted

  /** Set the name for the RDD; By default set to "SuccinctRDD" */
  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }

  setName("SuccinctRDD")

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

  override def getRecord(recordId: Long): Array[Byte] = {
    val recordIdRanges = partitionRecordIdRanges.filter(_.contains(recordId))
    if (recordIdRanges.size != 1) {
      throw new ArrayIndexOutOfBoundsException("Invalid recordId = " + recordId)
    }
    val recordIdRange = recordIdRanges(0)
    val records = partitionsRDD.map(partition => {
      if (partition.getFirstRecordId == recordIdRange.first) {
        partition.getRecord(recordId)
      } else {
        null
      }
    }).filter(buf => (buf != null)).collect
    if (records.size != 1) {
      throw new ArrayIndexOutOfBoundsException("Invalid recordId = " + recordId)
    }
    records(0)
  }

  /** Extract data from an RDD **/
  override def extract(offset: Long, length: Int): Array[Byte] = {
    val startPartitionRanges = partitionOffsetRanges.filter(_.contains(offset))
    val endPartitionRanges = partitionOffsetRanges.filter(_.contains(offset + length))

    if (startPartitionRanges.size != 1) {
      throw new ArrayIndexOutOfBoundsException("Invalid offset = " + offset)
    }

    if (endPartitionRanges.size != 1) {
      throw new ArrayIndexOutOfBoundsException("Invalid length = " + length)
    }

    val startPartitionRange = startPartitionRanges(0)
    val endPartitionRange = endPartitionRanges(0)

    // TODO: Handle case where extracted data spans more than 2 partitions

    if (startPartitionRange == endPartitionRange) {
      val values = partitionsRDD.map(partition => {
        if (partition.getFileOffset == startPartitionRange.first) {
          partition.extract(offset, length)
        } else {
          null
        }
      }
      ).filter(buf => (buf != null)).collect()
      values(0)
    } else {
      val startLength: Int = (startPartitionRange.second - offset).toInt
      val endLength: Int = length - startLength
      val values = partitionsRDD.map(partition => {
        if (partition.getFileOffset == startPartitionRange.first) {
          partition.extract(offset, startLength)
        } else if(partition.getFileOffset == endPartitionRange.first) {
          partition.extract(endPartitionRange.first, endLength)
        } else {
          null
        }
      }).filter(buf => (buf != null)).collect
      values(0) ++ values(1)
    }
  }
}
