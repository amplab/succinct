package edu.berkeley.cs.succinct.sql

import edu.berkeley.cs.succinct.SuccinctIndexedBuffer.QueryType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

/**
 * A container RDD for search results of a multi-search operation on [[SuccinctTableRDD]].
 *
 * @constructor Creates a [[MultiSearchResultsRDD]] from the underlying [[SuccinctTableRDD]], list of queries,
 *             list of separators and the target storage level for the RDD.
 * @param succinctTableRDD The underlying SuccinctTableRDD.
 * @param queryTypes The types of the queries.
 * @param queries The parameters for the queries.
 * @param reqColsCheck The list of required columns in the result.
 * @param succinctSerializer The serializer/deserializer for Succinct's representation of records.
 * @param targetStorageLevel The target storage level for the RDD.
 */
class MultiSearchResultsRDD(val succinctTableRDD: SuccinctTableRDD,
    val queryTypes: Array[QueryType],
    val queries: Array[Array[Array[Byte]]],
    val reqColsCheck: Map[String, Boolean],
    val succinctSerializer: SuccinctSerializer,
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends RDD[Row](succinctTableRDD.context, List(new OneToOneDependency(succinctTableRDD))) {

  /** Overrides the compute method in RDD to return an iterator over the search results. */
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val resultsIterator = succinctTableRDD.getFirstParent.iterator(split, context)
    if (resultsIterator.hasNext) {
      resultsIterator.next()
        .multiSearch(queryTypes, queries)
        .asInstanceOf[Array[Array[Byte]]]
        .map(succinctSerializer.deserializeRow(_, reqColsCheck))
        .iterator
    }
    else {
      Iterator[Row]()
    }
  }

  /**
   * Returns the partitions for the RDD.
   *
   * @return The array of partitions.
   */
  override def getPartitions: Array[Partition] = succinctTableRDD.partitions
}
