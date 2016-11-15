package edu.berkeley.cs.succinct.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

case class SuccinctPersistentRelation(location: String, userDefinedSchema: StructType = null)
                                     (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  val succinctTableRDD = SuccinctTableRDD(sqlContext.sparkContext, location).persist()
  private[succinct] var succinctSchema = getSchema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    succinctTableRDD.pruneAndFilter(requiredColumns, filters)
  }

  private[succinct] def getSchema: StructType = {
    val schemaPath = location.stripSuffix("/") + "/schema"
    val conf = sqlContext.sparkContext.hadoopConfiguration
    SuccinctUtils.readObjectFromFS[StructType](conf, schemaPath)
  }

  private[succinct] def getAttributeIdx(attribute: String): Int = {
    succinctSchema.lastIndexOf(schema(attribute))
  }

  override def schema: StructType = succinctSchema

}
