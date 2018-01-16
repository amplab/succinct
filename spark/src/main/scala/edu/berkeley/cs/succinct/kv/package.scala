package edu.berkeley.cs.succinct

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

package object kv {

  implicit class SuccinctContext(sc: SparkContext) {
    def succinctKV[K: ClassTag](filePath: String, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
                               (implicit ordering: Ordering[K])
    : SuccinctKVRDD[K] = SuccinctKVRDD[K](sc, filePath, storageLevel)

    def succinctStringKV[K: ClassTag](filePath: String, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
                                     (implicit ordering: Ordering[K])
    : SuccinctStringKVRDD[K] = SuccinctStringKVRDD[K](sc, filePath, storageLevel)
  }

  implicit class SuccinctSession(spark: SparkSession) {
    def succinctKV[K: ClassTag](filePath: String, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
                               (implicit ordering: Ordering[K])
    : SuccinctKVRDD[K] = SuccinctKVRDD[K](spark, filePath, storageLevel)

    def succinctStringKV[K: ClassTag](filePath: String, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
                                     (implicit ordering: Ordering[K])
    : SuccinctStringKVRDD[K] = SuccinctStringKVRDD[K](spark, filePath, storageLevel)
  }

  implicit class SuccinctPairedRDD[K: ClassTag](rdd: RDD[(K, Array[Byte])])
                                               (implicit ordering: Ordering[K]) {
    def succinctKV: SuccinctKVRDD[K] = {
      SuccinctKVRDD[K](rdd)
    }

    def saveAsSuccinctKV(path: String): Unit = SuccinctKVRDD[K](rdd).save(path)
  }

  implicit class SuccinctStringPairedRDD[K: ClassTag](rdd: RDD[(K, String)])
                                                     (implicit ordering: Ordering[K]) {
    def succinctKV: SuccinctStringKVRDD[K] = {
      SuccinctStringKVRDD[K](rdd)
    }

    def saveAsSuccinctKV(path: String): Unit = SuccinctStringKVRDD[K](rdd).save(path)
  }

}
