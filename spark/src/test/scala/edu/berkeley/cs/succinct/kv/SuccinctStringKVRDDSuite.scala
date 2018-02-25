package edu.berkeley.cs.succinct.kv

import com.google.common.io.Files
import org.apache.spark.LocalSparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class SuccinctStringKVRDDSuite extends FunSuite with LocalSparkContext {

  val conf = new SparkConf().setAppName("test").setMaster("local")
    .set("spark.driver.allowMultipleContexts", "true")

  def genKey(max: Int): String = String.valueOf(Math.abs(new Random().nextInt(max)))

  def genInt(max: Int): Int = Math.abs(new Random().nextInt(max))

  def search(data: String, str: String): Array[Int] = {
    var lastIndex = 0
    val results: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    while (lastIndex != -1) {
      lastIndex = data.indexOf(str, lastIndex)
      if (lastIndex != -1) {
        results += lastIndex
        lastIndex += str.length
      }
    }
    results.toArray
  }

  test("Test get") {
    sc = new SparkContext(conf)

    val textRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1))

    val succinctKVRDD = SuccinctStringKVRDD(kvRDD)
    val kvMap = kvRDD.collect().toMap

    val count = kvMap.size

    // Check
    (0 to 100).foreach(i => {
      val key = genKey(count)
      assert(kvMap(key) === succinctKVRDD.get(key))
    })
  }

  test("Test extract") {
    sc = new SparkContext(conf)

    val textRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1))

    val succinctKVRDD = SuccinctStringKVRDD(kvRDD)
    val kvMap = kvRDD.collect().toMap

    val count = kvMap.size

    // Check
    (0 to 100).foreach(i => {
      val key = genKey(count)
      if (kvMap(key).nonEmpty) {
        val offset = genInt(kvMap(key).length)
        val length = genInt(kvMap(key).length - offset)
        val recordData = succinctKVRDD.extract(key, offset, length)
        val expectedRecordData = new String(kvMap(key)).substring(offset, offset + length)
        assert(expectedRecordData === recordData)
      }
    })
  }

  test("Test search") {
    sc = new SparkContext(conf)

    val query = "TRUCK"

    val textRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1))

    val succinctKVRDD = SuccinctStringKVRDD(kvRDD)

    val expectedSearchResults = kvRDD.filter(t => new String(t._2).contains(query)).map(_._1).collect().sorted
    val searchResults = succinctKVRDD.search(query).collect().sorted

    assert(expectedSearchResults === searchResults)
  }

  test("Test count") {
    sc = new SparkContext(conf)

    val query = "TRUCK"

    val textRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1))
    val kvMap = kvRDD.collect().toMap

    val succinctKVRDD = SuccinctStringKVRDD(kvRDD)

    val expectedCount = kvMap.flatMap(t => search(new String(t._2), query)).size
    val count = succinctKVRDD.count(query)

    assert(expectedCount === count)
  }

  test("Test searchOffsets") {
    sc = new SparkContext(conf)

    val query = "TRUCK"

    val textRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1))
    val kvMap = kvRDD.collect().toMap

    val succinctKVRDD = SuccinctStringKVRDD(kvRDD)

    val expectedSearchResults = kvMap.flatMap(t =>
      search(new String(t._2), query).map(offset => (t._1, offset))).toArray.sorted
    val searchResults = succinctKVRDD.searchOffsets(query).collect().sorted

    assert(expectedSearchResults === searchResults)
  }

  test("Test multiple partitions") {
    sc = new SparkContext(conf)

    val query = "TRUCK"

    val textRDD = sc.textFile(getClass.getResource("/table.dat").getFile).repartition(5)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1))

    val succinctKVRDD = SuccinctStringKVRDD(kvRDD)
    val kvMap = kvRDD.collect().toMap

    val count = kvMap.size

    // Check get
    (0 to 100).foreach(i => {
      val key = genKey(count)
      assert(kvMap(key) === succinctKVRDD.get(key))
    })

    // Check extract
    (0 to 100).foreach(i => {
      val key = genKey(count)
      if (kvMap(key).nonEmpty) {
        val offset = genInt(kvMap(key).length)
        val length = genInt(kvMap(key).length - offset)
        val recordData = succinctKVRDD.extract(key, offset, length)
        val expectedRecordData = new String(kvMap(key)).substring(offset, offset + length)
        assert(expectedRecordData === recordData)
      }
    })

    // Check search
    val expectedSearchResults = kvRDD.filter(t => new String(t._2).contains(query)).map(_._1).collect().sorted
    val searchResults = succinctKVRDD.search(query).collect().sorted

    assert(expectedSearchResults === searchResults)

    // Check searchOffsets
    val expectedSearchOffsets = kvMap.flatMap(t =>
      search(new String(t._2), query).map(offset => (t._1, offset))).toArray.sorted
    val searchOffsets = succinctKVRDD.searchOffsets(query).collect().sorted

    assert(expectedSearchOffsets === searchOffsets)

  }

  test("Test bulkAppend") {
    sc = new SparkContext(conf)

    // TODO: Add more tests
    val textRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (t._2, t._1))
    val succinctKVRDD = SuccinctStringKVRDD(kvRDD)
    val count = kvRDD.count
    val newData = kvRDD.map(t => (t._1 + count, t._2))
    val newSuccinctKVRDD = succinctKVRDD.bulkAppend(newData)

    assert(kvRDD.count() * 2 == newSuccinctKVRDD.count())
  }

  test("Test save and load in memory") {
    sc = new SparkContext(conf)

    val textRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1))
    val succinctKVRDD = SuccinctStringKVRDD(kvRDD)

    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct"
    succinctKVRDD.save(succinctDir)

    val reloadedRDD = SuccinctStringKVRDD[String](sc, succinctDir, StorageLevel.MEMORY_ONLY)

    val originalKeys = succinctKVRDD.collect().map(_._1)
    val newKeys = reloadedRDD.collect().map(_._1)

    assert(originalKeys === newKeys)

    val originalValues = succinctKVRDD.collect().map(_._2)
    val newValues = reloadedRDD.collect().map(_._2)
    assert(originalValues === newValues)
  }

  test("Test save and load in memory 2") {
    sc = new SparkContext(conf)

    val textRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1))
    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct"
    kvRDD.saveAsSuccinctKV(succinctDir)

    val succinctKVRDD = kvRDD.succinctKV
    val reloadedRDD = sc.succinctStringKV[String](succinctDir)

    val originalKeys = succinctKVRDD.collect().map(_._1)
    val newKeys = reloadedRDD.collect().map(_._1)

    assert(originalKeys === newKeys)

    val originalValues = succinctKVRDD.collect().map(_._2)
    val newValues = reloadedRDD.collect().map(_._2)
    assert(originalValues === newValues)
  }

  test("Test save and load in memory 3") {
    sc = new SparkContext(conf)

    val textRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1)).repartition(5)
    val succinctKVRDD = SuccinctStringKVRDD(kvRDD, true)

    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct"
    succinctKVRDD.save(succinctDir)

    val reloadedRDD = SuccinctStringKVRDD[String](sc, succinctDir, StorageLevel.MEMORY_ONLY)

    val originalKeys = succinctKVRDD.collect().map(_._1)
    val newKeys = reloadedRDD.collect().map(_._1)

    assert(originalKeys === newKeys)

    val originalValues = succinctKVRDD.collect().map(_._2)
    val newValues = reloadedRDD.collect().map(_._2)
    assert(originalValues === newValues)
  }

  test("Test save and load in memory 4") {
    sc = new SparkContext(conf)

    val textRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
    val kvRDD = textRDD.zipWithIndex().map(t => (String.valueOf(t._2), t._1)).sortByKey()

    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct"

    SuccinctStringKVRDD.constructAndSave(kvRDD, succinctDir)

    val reloadedRDD = SuccinctStringKVRDD[String](sc, succinctDir, StorageLevel.MEMORY_ONLY)

    val originalKeys = kvRDD.collect().map(_._1)
    val newKeys = reloadedRDD.collect().map(_._1)

    assert(originalKeys === newKeys)

    val originalValues = kvRDD.collect().map(_._2)
    val newValues = reloadedRDD.collect().map(_._2)
    assert(originalValues === newValues)
  }

}
