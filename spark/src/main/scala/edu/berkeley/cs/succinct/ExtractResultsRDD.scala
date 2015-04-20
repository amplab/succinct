package edu.berkeley.cs.succinct

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

/**
 * Container RDD for the extract results of calling extractRecords on SuccinctRDD. The extract results are lazily
 * evaluated.
 *
 * @constructor Creates an ExtractResultsRDD from the underlying SuccinctRDD, extract offset and length, and the target
 *              storage level for the RDD.
 * @param succinctRDD The underlying SuccinctRDD.
 * @param eOffset The extract offset.
 * @param eLen The extract length.
 * @param targetStorageLevel The target storage level.
 */
class ExtractResultsRDD(val succinctRDD: SuccinctRDD,
    val eOffset: Int,
    val eLen: Int,
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends RDD[Array[Byte]](succinctRDD.context, List(new OneToOneDependency(succinctRDD))) {

  /** Overrides the compute method of RDD to return an iterator over the extracted data. */
  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    succinctRDD.getFirstParent
      .iterator(split, context)
      .next
      .extractRecords(eOffset, eLen)
      .asInstanceOf[Array[Array[Byte]]]
      .iterator

  }

  /**
   * Returns the array of partitions.
   *
   * @return The array of partitions.
   */
  override def getPartitions: Array[Partition] = succinctRDD.partitions
}
