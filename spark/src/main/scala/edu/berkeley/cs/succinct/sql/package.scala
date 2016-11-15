package edu.berkeley.cs.succinct

import org.apache.spark.sql.{SparkSession, DataFrame, SQLContext}

package object sql {

  /**
    * Adds a method, `succinctTable`, to SQLContext that allows reading data stored in Succinct format.
    */
  implicit class SuccinctSQLContext(sqlContext: SQLContext) {
    def succinctTable(filePath: String) = {
      sqlContext.baseRelationToDataFrame(SuccinctPersistentRelation(filePath)(sqlContext))
    }
  }

  /**
    * Adds a method, `succinctTable`, to SparkSession that allows reading data stored in Succinct format.
    */
  implicit class SuccinctSession(spark: SparkSession) {
    def succinctTable(filePath: String) = {
      spark.baseRelationToDataFrame(SuccinctPersistentRelation(filePath)(spark.sqlContext))
    }
  }

  /**
    * Adds method `saveAsSuccinctTable` to DataFrame that permits saving it in Succinct format.
    * Adds method `toSuccinctDf` to DataFrame that permits converting it to Succinct format.
    */
  implicit class SuccinctDataFrame(dataFrame: DataFrame) {
    def saveAsSuccinctTable(path: String): Unit = SuccinctTableRDD(dataFrame).save(path)
    def toSuccinctDF = {
      val spark = dataFrame.sparkSession
      spark.baseRelationToDataFrame(SuccinctInMemoryRelation(dataFrame)(spark.sqlContext))
    }
  }

}
