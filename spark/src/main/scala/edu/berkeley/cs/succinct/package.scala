package edu.berkeley.cs

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

package object succinct {

  implicit class SuccinctContext(sc: SparkContext) {
    def succinctFile(filePath: String, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): SuccinctRDD = {
      SuccinctRDD(sc, filePath, storageLevel)
    }
  }

  implicit class SuccinctSession(spark: SparkSession) {
    def succinctFile(filePath: String, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): SuccinctRDD = {
      SuccinctRDD(spark, filePath, storageLevel)
    }
  }

  implicit class SuccinctFlatRDD(rdd: RDD[Array[Byte]]) {
    def succinct: SuccinctRDD = {
      SuccinctRDD(rdd)
    }

    def saveAsSuccinctFile(path: String): Unit = SuccinctRDD(rdd).save(path)
  }

}
