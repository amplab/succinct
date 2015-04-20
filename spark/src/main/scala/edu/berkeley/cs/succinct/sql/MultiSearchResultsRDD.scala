package edu.berkeley.cs.succinct.sql

import edu.berkeley.cs.succinct.SuccinctIndexedBuffer.QueryType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
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
 * @param requiredColumns The list of required columns in the result.
 * @param separators The list of separators.
 * @param schema The schema for the table.
 * @param targetStorageLevel The target storage level for the RDD.
 */
class MultiSearchResultsRDD(val succinctTableRDD: SuccinctTableRDD,
    val queryTypes: Array[QueryType],
    val queries: Array[Array[Array[Byte]]],
    val requiredColumns: Array[String],
    val separators: Array[Byte],
    val schema: StructType,
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends RDD[Row](succinctTableRDD.context, List(new OneToOneDependency(succinctTableRDD))) {

  val reqColsCheck = schema.map(f => f.name -> requiredColumns.contains(f.name)).toMap

  /** Overrides the compute method in RDD to return an iterator over the search results. */
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    succinctTableRDD.getFirstParent
      .iterator(split, context)
      .next
      .multiSearch(queryTypes, queries)
      .asInstanceOf[Array[Array[Byte]]]
      .iterator
      .map(SuccinctSerializer.deserializeRow(_, separators, schema, reqColsCheck))
  }

  /**
   * Returns the partitions for the RDD.
   *
   * @return The array of partitions.
   */
  override def getPartitions: Array[Partition] = succinctTableRDD.partitions
}
