package edu.berkeley.cs.succinct.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

/**
 * A container RDD for search results of a search operation on [[SuccinctTableRDD]].
 *
 * @constructor Creates a [[SearchResultsRDD]] from the underlying [[SuccinctTableRDD]], query, list of separators
 *              and the target storage level for the RDD.
 * @param succinctTableRDD The underlying SuccinctTableRDD.
 * @param searchQuery The search query.
 * @param separators The list of separators.
 * @param schema The schema for the table.
 * @param targetStorageLevel The target storage level for the RDD.
 */
class SearchResultsRDD(val succinctTableRDD: SuccinctTableRDD,
    val searchQuery: Array[Byte],
    val separators: Array[Byte],
    val schema: StructType,
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends RDD[Row](succinctTableRDD.context, List(new OneToOneDependency(succinctTableRDD))) {

  /** Overrides the compute method in RDD to return an iterator over the search results. */
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    succinctTableRDD.getFirstParent
      .iterator(split, context)
      .next
      .recordSearch(searchQuery)
      .asInstanceOf[Array[Array[Byte]]]
      .iterator
      .map(SuccinctSerializer.deserializeRow(_, separators, schema))
  }

  /**
   * Returns the partitions for the RDD.
   *
   * @return The array of partitions.
   */
  override def getPartitions: Array[Partition] = succinctTableRDD.partitions
}
