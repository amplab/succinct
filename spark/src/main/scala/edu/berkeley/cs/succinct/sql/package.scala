package edu.berkeley.cs.succinct

import org.apache.spark.sql.{SparkSession, DataFrame, SQLContext}

package object sql {

  /**
    * Adds a method, `succinctTable`, to SQLContext that allows reading data stored in Succinct format.
    */
  implicit class SuccinctSQLContext(sqlContext: SQLContext) {
    def succinctTable(filePath: String) = {
      sqlContext.baseRelationToDataFrame(SuccinctRelation(filePath)(sqlContext))
    }
  }

  /**
    * Adds a method, `succinctTable`, to SparkSession that allows reading data stored in Succinct format.
    */
  implicit class SuccinctSession(spark: SparkSession) {
    def succinctTable(filePath: String) = {
      spark.baseRelationToDataFrame(SuccinctRelation(filePath)(spark.sqlContext))
    }
  }

  /**
    * Adds a method, `saveAsSuccinctTable`, to DataFrame that allows you to save it in Succinct format.
    */
  implicit class SuccinctDataFrame(dataFrame: DataFrame) {
    def saveAsSuccinctTable(path: String): Unit = SuccinctTableRDD(dataFrame).save(path)
  }

}
