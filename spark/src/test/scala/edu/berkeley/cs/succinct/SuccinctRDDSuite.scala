package edu.berkeley.cs.succinct

import com.google.common.io.Files
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class SuccinctRDDSuite extends FunSuite with LocalSparkContext {

  def search(data: String, str: String): Array[Long] = {
    var lastIndex = 0L
    val results: ArrayBuffer[Long] = new ArrayBuffer[Long]()
    while (lastIndex != -1) {
      lastIndex = data.indexOf(str, lastIndex.toInt).toLong
      if (lastIndex != -1) {
        results += lastIndex
        lastIndex += str.length
      }
    }
    results.toArray
  }

  test("Test searchOffsets") {
    sc = new SparkContext("local", "test")

    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val data = textRDD.collect().mkString("\n")
    val expectedSearchOffsets = search(data, query)

    // Compute results
    val searchOffsets = succinctRDD.searchOffsets(query).collect().sorted

    assert(searchOffsets === expectedSearchOffsets)
  }

  test("Test countOffsets") {
    sc = new SparkContext("local", "test")

    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected value
    val partitionsArray = textRDD.mapPartitions(data => Iterator(data.mkString("\n"))).collect()
    val expectedCount = partitionsArray.map(data => search(data, query).length).aggregate(0L)(_ + _, _ + _)

    // Compute result
    val count = succinctRDD.countOffsets(query)

    assert(count === expectedCount)
  }

  test("Test extract") {
    sc = new SparkContext("local", "test")

    val offset = 100
    val length = 100
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val data = textRDD.collect().mkString("\n")
    val expectedExtractedData = data.substring(offset, offset + length).getBytes

    // Compute results
    val extractedData = succinctRDD.extract(offset, length)

    assert(extractedData === expectedExtractedData)
  }

  test("Test search") {
    sc = new SparkContext("local", "test")

    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val expectedSearchRecords = textRDD.filter(_.contains(query)).collect().sorted

    // Compute results
    val searchRecords = succinctRDD.search(query).records().collect().map(new String(_)).sorted

    assert(searchRecords === expectedSearchRecords)
  }

  test("Test regexSearch") {
    sc = new SparkContext("local", "test")

    // TODO: Add more tests
    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val expectedSearchRecords = textRDD.filter(_.contains(query)).collect().sorted

    // Compute results
    val searchRecords = succinctRDD.regexSearch(query).collect().map(new String(_)).sorted

    assert(searchRecords.length == expectedSearchRecords.length)
    assert(searchRecords === expectedSearchRecords)
  }

  test("Test RDD count") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val expectedCount = textRDD.count()

    // Compute results
    val count = succinctRDD.count()

    assert(count === expectedCount)
  }

  test("Test multiple partitions") {
    // TODO: Add tests

    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile).repartition(5)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    val query = "int"
    val offset = 100
    val length = 100

    // Compute expected values
    val data = textRDD.collect().mkString("\n")
    val expectedSearchOffsets = search(data, query)

    // Compute results
    val searchOffsets = succinctRDD.searchOffsets(query).collect().sorted

    assert(searchOffsets === expectedSearchOffsets)

    // Compute expected value
    val expectedCount = expectedSearchOffsets.length

    // Compute result
    val count = succinctRDD.countOffsets(query)

    assert(count === expectedCount)

    // Compute expected values
    val expectedExtractedData = data.substring(offset, offset + length).getBytes

    // Compute results
    val extractedData = succinctRDD.extract(offset, length)

    assert(extractedData === expectedExtractedData)
  }

  test("Test save and load in memory") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes)).persist()

    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct"
    succinctRDD.save(succinctDir)

    val originalEntries = succinctRDD.collect()
    val newEntries = SuccinctRDD(sc, succinctDir, StorageLevel.MEMORY_ONLY).collect()

    assert(originalEntries === newEntries)
  }

}
