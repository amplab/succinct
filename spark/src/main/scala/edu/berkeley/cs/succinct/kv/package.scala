package edu.berkeley.cs.succinct

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

package object kv {

  implicit class SuccinctContext(sc: SparkContext) {
    def succinctKV[K: ClassTag](filePath: String, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
                               (implicit ordering: Ordering[K])
    : SuccinctKVRDD[K] = SuccinctKVRDD[K](sc, filePath, storageLevel)
  }

  implicit class SuccinctPairedRDD[K: ClassTag](rdd: RDD[(K, Array[Byte])])
                                               (implicit ordering: Ordering[K]) {
    def succinctKV: SuccinctKVRDD[K] = {
      SuccinctKVRDD[K](rdd)
    }

    def saveAsSuccinctKV(path: String): Unit = SuccinctKVRDD[K](rdd).save(path)
  }

}
