package edu.berkeley.cs.succinct

import org.apache.spark.sql.{DataFrame, SQLContext}

package object sql {

  /**
   * Adds a method, `succinctFile`, to SQLContext that allows reading data stored in Succinct format.
   */
  implicit class SuccinctContext(sqlContext: SQLContext) {
    def succinctFile(filePath: String) = {
      sqlContext.baseRelationToDataFrame(SuccinctRelation(filePath)(sqlContext))
    }
  }

  /**
   * Adds a method, `saveAsSuccinctFile`, to DataFrame that allows you to save it in Succinct format.
   */
  implicit class SuccinctDataFrame(dataFrame: DataFrame) {
    def saveAsSuccinctFiles(path: String): Unit = SuccinctTableRDD(dataFrame).save(path)
  }
}
