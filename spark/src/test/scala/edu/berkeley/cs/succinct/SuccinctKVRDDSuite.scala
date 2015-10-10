package edu.berkeley.cs.succinct

import com.google.common.io.Files
import edu.berkeley.cs.succinct.kv.SuccinctKVRDD
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.scalatest.FunSuite

class SuccinctKVRDDSuite extends FunSuite with LocalSparkContext {

  test("Test get") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1.getBytes()))

    val succinctKVRDD = SuccinctKVRDD(kvRDD)
    val kvMap = kvRDD.collect().toMap

    // Check
    (0 to 1000).foreach(i => {
      val key = String.valueOf(i)
      assert(kvMap(key) === succinctKVRDD.get(key))
    })
  }

  test("Test delete") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1.getBytes()))

    val succinctKVRDD = SuccinctKVRDD(kvRDD)

    // Check
    (0 to 1000).foreach(i => {
      val key = String.valueOf(i)
      assert(succinctKVRDD.delete(key))
      assert(succinctKVRDD.get(key) == null)
    })
  }

  test("Test multiple partitions") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile).repartition(5)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1.getBytes()))

    val succinctKVRDD = SuccinctKVRDD(kvRDD)
    val kvMap = kvRDD.collect().toMap

    // Check
    (0 to 1000).foreach(i => {
      val key = String.valueOf(i)
      assert(kvMap(key) === succinctKVRDD.get(key))
    })

    // Check
    (0 to 1000).foreach(i => {
      val key = String.valueOf(i)
      assert(succinctKVRDD.delete(key))
      assert(succinctKVRDD.get(key) == null)
    })
  }

  test("Test save and load in memory") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1.getBytes()))
    val succinctKVRDD = SuccinctKVRDD(kvRDD)

    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct"
    succinctKVRDD.save(succinctDir)

    val reloadedRDD = SuccinctKVRDD[String](sc, succinctDir, StorageLevel.MEMORY_ONLY)

    val originalKeys = succinctKVRDD.collect().map(_._1)
    val newKeys = reloadedRDD.collect().map(_._1)

    assert(originalKeys === newKeys)

    val originalValues = succinctKVRDD.collect().map(_._2)
    val newValues = reloadedRDD.collect().map(_._2)
    assert(originalValues === newValues)
  }

}
