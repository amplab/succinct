package edu.berkeley.cs.succinct.kv

import com.google.common.io.Files
import edu.berkeley.cs.succinct.LocalSparkContext
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.scalatest.FunSuite

import scala.util.Random

class SuccinctKVRDDSuite extends FunSuite with LocalSparkContext {

  def genKey(max: Int): String = String.valueOf(Math.abs(new Random().nextInt(max)))

  def genInt(max: Int): Int = Math.abs(new Random().nextInt(max))

  test("Test get") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1.getBytes))

    val succinctKVRDD = SuccinctKVRDD(kvRDD)
    val kvMap = kvRDD.collect().toMap

    val count = kvMap.size

    // Check
    (0 to 100).foreach(i => {
      val key = genKey(count)
      assert(kvMap(key) === succinctKVRDD.get(key))
    })
  }

  test("Test extract") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1.getBytes))

    val succinctKVRDD = SuccinctKVRDD(kvRDD)
    val kvMap = kvRDD.collect().toMap

    val count = kvMap.size

    // Check
    (0 to 100).foreach(i => {
      val key = genKey(count)
      if (kvMap(key).nonEmpty) {
        val offset = genInt(kvMap(key).length)
        val length = genInt(kvMap(key).length - offset)
        val recordData = succinctKVRDD.access(key, offset, length)
        val expectedRecordData = new String(kvMap(key)).substring(offset, offset + length).getBytes
        assert(expectedRecordData === recordData)
      }
    })
  }

  test("search") {
    sc = new SparkContext("local", "test")

    val query = "TRUCK"

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1.getBytes))

    val succinctKVRDD = SuccinctKVRDD(kvRDD)

    val expectedSearchResults = kvRDD.filter(t => new String(t._2).contains(query)).map(_._1).collect()
    val searchResults = succinctKVRDD.search(query).collect()

    assert(expectedSearchResults === searchResults)
  }

  test("Test multiple partitions") {
    sc = new SparkContext("local", "test")

    val query = "TRUCK"

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile).repartition(5)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1.getBytes))

    val succinctKVRDD = SuccinctKVRDD(kvRDD)
    val kvMap = kvRDD.collect().toMap

    val count = kvMap.size

    // Check
    (0 to 100).foreach(i => {
      val key = genKey(count)
      assert(kvMap(key) === succinctKVRDD.get(key))
    })

    // Check
    (0 to 100).foreach(i => {
      val key = genKey(count)
      if (kvMap(key).nonEmpty) {
        val offset = genInt(kvMap(key).length)
        val length = genInt(kvMap(key).length - offset)
        val recordData = succinctKVRDD.access(key, offset, length)
        val expectedRecordData = new String(kvMap(key)).substring(offset, offset + length).getBytes
        assert(expectedRecordData === recordData)
      }
    })

    val expectedSearchResults = kvRDD.filter(t => new String(t._2).contains(query)).map(_._1).collect()
    val searchResults = succinctKVRDD.search(query).collect()

    assert(expectedSearchResults === searchResults)
  }

  test("Test save and load in memory") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1.getBytes))
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
