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
  extends RDD[Long](succinctRDD.context, List(new OneToOneDependency(succinctRDD))) {

  /**
   * Overrides the compute method of RDD to return an iterator over the search results
   * (offsets into the partition).
     s*/
  override def compute(split: Partition, context: TaskContext): Iterator[Long] = {
    val resultsIterator = succinctRDD.getFirstParent.iterator(split, context)
    if (resultsIterator.hasNext) {
      resultsIterator.next()
        .recordSearchOffsets(searchQuery)
        .map(Long2long)
        .iterator
    } else {
      Iterator[Long]()
    }
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
