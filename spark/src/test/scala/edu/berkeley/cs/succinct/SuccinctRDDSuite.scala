package edu.berkeley.cs.succinct

import com.google.common.io.Files
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext, LocalSparkContext}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class SuccinctRDDSuite extends FunSuite with LocalSparkContext {

  val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
    .set("spark.driver.allowMultipleContexts", "true")

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

  test("Test search") {
    sc = new SparkContext(conf)

    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val data = textRDD.collect().mkString("\n")
    val expectedSearchOffsets = search(data, query)

    // Compute results
    val searchOffsets = succinctRDD.search(query).collect().sorted

    assert(searchOffsets === expectedSearchOffsets)
  }

  test("Test count") {
    sc = new SparkContext(conf)

    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected value
    val partitionsArray = textRDD.mapPartitions(data => Iterator(data.mkString("\n"))).collect()
    val expectedCount = partitionsArray.map(data => search(data, query).length).aggregate(0L)(_ + _, _ + _)

    // Compute result
    val count = succinctRDD.count(query)

    assert(count === expectedCount)
  }

  test("Test extract") {
    sc = new SparkContext(conf)

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

  test("Test regexSearch") {
    sc = new SparkContext(conf)

    // TODO: Add more tests
    val query = "int"
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    // Compute expected values
    val data = textRDD.collect().mkString("\n")
    val expectedSearchOffsets = search(data, query)

    // Compute results
    val searchOffsets = succinctRDD.regexSearch(query).map(_.getOffset).collect().sorted

    assert(searchOffsets.length == expectedSearchOffsets.length)
    assert(searchOffsets === expectedSearchOffsets)
  }

  test("Test bulkAppend") {
    sc = new SparkContext(conf)

    // TODO: Add more tests
    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))
    val newSuccinctRDD = succinctRDD.bulkAppend(textRDD.map(_.getBytes))

    assert(textRDD.count() * 2 == newSuccinctRDD.count())
  }

  test("Test RDD count") {
    sc = new SparkContext(conf)

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

    sc = new SparkContext(conf)

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile).repartition(5)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes))

    val query = "int"
    val offset = 100
    val length = 100

    // Compute expected values
    val data = textRDD.collect().mkString("\n")
    val expectedSearchOffsets = search(data, query)

    // Compute results
    val searchOffsets = succinctRDD.search(query).collect().sorted

    assert(searchOffsets === expectedSearchOffsets)

    // Compute expected value
    val expectedCount = expectedSearchOffsets.length

    // Compute result
    val count = succinctRDD.count(query)

    assert(count === expectedCount)

    // Compute expected values
    val expectedExtractedData = data.substring(offset, offset + length).getBytes

    // Compute results
    val extractedData = succinctRDD.extract(offset, length)
    assert(extractedData === expectedExtractedData)

    // Save and load only one partition
    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct"
    succinctRDD.save(succinctDir)

    val loadData = SuccinctRDD(sc, succinctDir + "/" + "part-00000", StorageLevel.MEMORY_ONLY).collect()
    val expectedData = succinctRDD.mapPartitionsWithIndex((i, p) => {
      if(i == 0) p else Iterator[Array[Byte]]()
    }).collect()
    assert(loadData === expectedData)
  }

  test("Test save and load in memory") {
    sc = new SparkContext(conf)

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile)
    val succinctRDD = SuccinctRDD(textRDD.map(_.getBytes)).persist()

    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct"
    succinctRDD.save(succinctDir)

    val originalEntries = succinctRDD.collect()
    val newEntries = SuccinctRDD(sc, succinctDir, StorageLevel.MEMORY_ONLY).collect()

    assert(originalEntries === newEntries)
  }

  test("Test save and load in memory 2") {
    sc = new SparkContext(conf)

    val textRDD = sc.textFile(getClass.getResource("/raw.dat").getFile).map(_.getBytes)
    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct"
    textRDD.saveAsSuccinctFile(succinctDir)

    val succinctRDD = textRDD.succinct

    val originalEntries = succinctRDD.collect()
    val newEntries = sc.succinctFile(succinctDir).collect()

    assert(originalEntries === newEntries)
  }

}
