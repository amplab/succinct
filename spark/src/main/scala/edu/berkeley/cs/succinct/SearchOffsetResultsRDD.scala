package edu.berkeley.cs.succinct

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

/**
 * A container RDD for the search results as offsets into the original partition of a SuccinctRDD. The results
 * are lazily evaluated.
 *
 * @constructor Creates a SuccinctOffsetrResultsRDD from the underlying SuccinctRDD, the search query and the target
 *              storage level.
 * @param succinctRDD The underlying SuccinctRDD.
 * @param searchQuery The search query.
 * @param targetStorageLevel The target storage level for the RDD.
 */
class SearchOffsetResultsRDD(val succinctRDD: SuccinctRDD,
    val searchQuery: Array[Byte],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends RDD[Array[Long]](succinctRDD.context, List(new OneToOneDependency(succinctRDD))) {

  /**
   * Overrides the compute method of RDD to return an iterator over the search results
   * (offsets into the partition).
     s*/
  override def compute(split: Partition, context: TaskContext): Iterator[Array[Long]] = {

    Iterator(succinctRDD.getFirstParent
      .iterator(split, context)
      .next
      .recordSearchOffsets(searchQuery)
      .toArray
      .map(Long2long)
      .asInstanceOf[Array[Long]])

  }

  /**
   * Returns the array of partitions.
   *
   * @return The array of partitions.
   */
  override def getPartitions: Array[Partition] = succinctRDD.partitions

  /**
   * Converts the offsets RDD to records RDD.
   *
   * @return The corresponding SearchRecordResultsRDD.
   */
  def records(): SearchRecordResultsRDD = {
    new SearchRecordResultsRDD(succinctRDD, searchQuery, targetStorageLevel)
  }
}
