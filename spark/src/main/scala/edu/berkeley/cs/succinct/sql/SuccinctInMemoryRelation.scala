package edu.berkeley.cs.succinct.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class SuccinctInMemoryRelation(dataFrame: DataFrame)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  private[succinct] val succinctTableRDD = SuccinctTableRDD(dataFrame).persist()
  private[succinct] val succinctSchema = dataFrame.schema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    succinctTableRDD.pruneAndFilter(requiredColumns, filters)
  }

  override def schema: StructType = succinctSchema

}
