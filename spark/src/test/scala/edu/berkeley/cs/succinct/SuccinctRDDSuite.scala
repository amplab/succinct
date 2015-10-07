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
    val data = textRDD.collect.mkString("\n")
    val expectedSearchOffsets = search(data, query)

    // Compute results
    val searchOffsets = succinctRDD.searchOffsets(query).collect.sorted

    assert(searchOffsets === expectedSearchOffsets)
  }

  test("Test countOffsets") {
    sc = new SparkContext("local", "test")

    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected value
    val partitionsArray = textRDD.mapPartitions(data => Iterator(data.mkString("\n"))).collect
    val expectedCount = partitionsArray.map(data => search(data, query).size).aggregate(0L)(_ + _, _ + _)

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
    val data = textRDD.collect.mkString("\n")
    val expectedExtractedData = data.substring(offset, offset + length).getBytes

    // Compute results
    val extractedData = succinctRDD.extract(offset, length)

    assert(extractedData === expectedExtractedData)
  }

  test("Test extractPerPartition") {
    sc = new SparkContext("local", "test")

    val offset = 0
    val len = 10
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val partitionsArray = textRDD.mapPartitions(data => Iterator(data.mkString("\n"))).collect
    val expectedExtracts = partitionsArray.map(_.substring(offset, offset + len).getBytes)

    // Compute results
    val extracts = succinctRDD.extractPerPartition(offset, len).collect

    assert(expectedExtracts === extracts)
  }

  test("Get record") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    val records = textRDD.map(_.getBytes).collect

    // Check
    (0 to 1000).foreach(i => {
      assert(records(i) === succinctRDD.getRecord(i))
    })
  }

  test("Test search") {
    sc = new SparkContext("local", "test")

    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val expectedSearchRecords = textRDD.filter(_.contains(query)).collect.sorted

    // Compute results
    val searchRecords = succinctRDD.search(query).records().collect.map(new String(_)).sorted

    assert(searchRecords === expectedSearchRecords)
  }

  test("Test count") {
    sc = new SparkContext("local", "test")

    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val expectedCount = textRDD.filter(_.contains(query)).count

    // Compute results
    val count = succinctRDD.count(query)

    assert(count === expectedCount)
  }

  test("Test extractRecords") {
    sc = new SparkContext("local", "test")

    val offset = 0
    val len = 10
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile).filter(_.length > 10)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val expectedExtracts = textRDD.map(data => data.substring(offset, len).getBytes).collect

    // Compute results
    val extracts = succinctRDD.extractRecords(offset, len).collect

    assert(expectedExtracts === extracts)
  }

  test("Test regexSearch") {
    sc = new SparkContext("local", "test")

    // TODO: Add more tests
    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val expectedSearchRecords = textRDD.filter(_.contains(query)).collect.sorted

    // Compute results
    val searchRecords = succinctRDD.regexSearch(query).collect.map(new String(_)).sorted

    assert(searchRecords.size == expectedSearchRecords.size)
    assert(searchRecords === expectedSearchRecords)
  }

  test("Test RDD count") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val expectedCount = textRDD.count

    // Compute results
    val count = succinctRDD.count

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
    val data = textRDD.collect.mkString("\n")
    val expectedSearchOffsets = search(data, query)

    // Compute results
    val searchOffsets = succinctRDD.searchOffsets(query).collect.sorted

    assert(searchOffsets === expectedSearchOffsets)

    // Compute expected value
    val expectedCount = expectedSearchOffsets.size

    // Compute result
    val count = succinctRDD.countOffsets(query)

    assert(count === expectedCount)

    // Compute expected values
    val expectedExtractedData = data.substring(offset, offset + length).getBytes

    // Compute results
    val extractedData = succinctRDD.extract(offset, length)

    assert(extractedData === expectedExtractedData)

    // Compute expected values
    val partitionsArray = textRDD.mapPartitions(data => Iterator(data.mkString("\n"))).collect
    val expectedExtracts = partitionsArray.map(_.substring(offset, offset + length).getBytes)

    // Compute results
    val extracts = succinctRDD.extractPerPartition(offset, length).collect

    assert(expectedExtracts === extracts)

    val records = textRDD.map(_.getBytes).collect

    // Check
    (0 to 1000).foreach(i => {
      assert(records(i) === succinctRDD.getRecord(i))
    })
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
