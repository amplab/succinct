package edu.berkeley.cs.succinct

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

/**
 * A container RDD for the search results (i.e., actual records) of a search query on a SuccinctRDD. The results are
 * lazily evaluated.
 *
 * @constructor Creates a SuccinctRecordResultsRDD from the underlying SuccinctRDD, query and target storage level
 *              for the RDD.
 * @param succinctRDD The underlying SuccinctRDD.
 * @param searchQuery The search query.
 * @param targetStorageLevel The target storage level for the RDD.
 */
class SearchRecordResultsRDD(val succinctRDD: SuccinctRDD,
                             val searchQuery: Array[Byte],
                             val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends RDD[Array[Byte]](succinctRDD.context, List(new OneToOneDependency(succinctRDD))) {

  /** Overrides the compute method of RDD to return an iterator over the search results. */
  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val resultsIterator = succinctRDD.getFirstParent.iterator(split, context)
    if (resultsIterator.hasNext) {
      resultsIterator.next().searchRecords(searchQuery)
    } else {
      Iterator[Array[Byte]]()
    }
  }

  /**
   * Returns the array of partitions.
   *
   * @return The array of partitions.
   */
  override def getPartitions: Array[Partition] = succinctRDD.partitions

  /**
   * Converts the records RDD to the recordIds RDD.
   *
   * @return The corresponding SearchRecordIdResultsRDD.
   */
  def recordIds(): SearchRecordIdResultsRDD = {
    new SearchRecordIdResultsRDD(succinctRDD, searchQuery, targetStorageLevel)
  }

  /**
   * Converts to an RDD of String representation.
   * @return An RDD of strings.
   */
  def toStringRDD: RDD[String] = {
    map(new String(_))
  }
}
