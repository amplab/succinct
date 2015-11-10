package edu.berkeley.cs.succinct

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

package object json {

  implicit class SuccinctContext(sc: SparkContext) {
    def succinctJson(filePath: String, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : SuccinctJsonRDD = SuccinctJsonRDD(sc, filePath, storageLevel)
  }

  implicit class SuccinctJSONRDD(rdd: RDD[String]) {
    def succinctJson: SuccinctJsonRDD = {
      SuccinctJsonRDD(rdd)
    }

    def saveAsSuccinctJson(path: String): Unit = SuccinctJsonRDD(rdd).save(path)
  }

}
