package edu.berkeley.cs.succinct

import com.google.common.io.Files
import org.apache.spark.SparkContext
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class SuccinctRDDSuite extends FunSuite with LocalSparkContext {
  
  def search(data: String, str: String): Iterable[Long] = {
    var lastIndex = 0L
    val results:ArrayBuffer[Long] = new ArrayBuffer[Long]()
    while (lastIndex != -1) {
      lastIndex = data.indexOf(str, lastIndex.toInt).toLong
      if (lastIndex != -1) {
        results += lastIndex
        lastIndex += str.length
      }
    }
    results.toIterable
  }
  
  test("Test search") {
    sc = new SparkContext("local", "test")
    
    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))
    
    // Compute expected values
    val partitionsArray = textRDD.mapPartitions(data => Iterator(data.mkString("\n"))).collect
    val expectedSearchOffsets = partitionsArray.map(data => search(data, query))

    // Compute results
    val searchOffsets = succinctRDD.search(query.getBytes).collect.map(t => t.toList.sorted.toIterable)
    
    assert(searchOffsets === expectedSearchOffsets)
  }
  
  test("Test count") {
    sc = new SparkContext("local", "test")

    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected value
    val partitionsArray = textRDD.mapPartitions(data => Iterator(data.mkString("\n"))).collect
    val expectedCount = partitionsArray.map(data => search(data, query).size).aggregate(0L)(_ + _, _ + _)

    // Compute result
    val count = succinctRDD.count(query.getBytes)

    assert(count === expectedCount)
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
  
  test("Test searchRecords") {
    sc = new SparkContext("local", "test")

    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val expectedSearchRecords = textRDD.filter(_.contains(query)).collect.sorted

    // Compute results
    val searchRecords = succinctRDD.searchRecords(query.getBytes).records().collect.map(new String(_)).sorted

    assert(searchRecords === expectedSearchRecords)
  }
  
  test("Test countRecords") {
    sc = new SparkContext("local", "test")

    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val expectedCount = textRDD.filter(_.contains(query)).count

    // Compute results
    val count = succinctRDD.countRecords(query.getBytes)

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

  test("Test regexSearchRecords") {
    sc = new SparkContext("local", "test")

    // TODO: Add more tests
    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val expectedSearchRecords = textRDD.filter(_.contains(query)).collect.sorted

    // Compute results
    val searchRecords = succinctRDD.regexSearchRecords(query).collect.map(new String(_)).sorted
    
    assert(searchRecords.size == expectedSearchRecords.size)
    assert(searchRecords === expectedSearchRecords)
  }

  test("Test rdd count") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val expectedCount = textRDD.count

    // Compute results
    val count = succinctRDD.count

    assert(count === expectedCount)
  }

  test("Test save and load") {
    sc = new SparkContext("local", "test")

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes)).persist()

    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct"
    succinctRDD.save(succinctDir)

    val originalEntries = succinctRDD.collect()
    val newEntries = SuccinctRDD(sc, succinctDir).collect()

    assert(originalEntries === newEntries)
  }
}
