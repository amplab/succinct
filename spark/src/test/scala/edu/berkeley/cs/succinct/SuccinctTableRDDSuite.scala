package edu.berkeley.cs.succinct

import com.google.common.io.Files
import edu.berkeley.cs.succinct.sql.SuccinctTableRDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSuite

class SuccinctTableRDDSuite extends FunSuite with LocalSparkContext {

  test("Test search") {
    sc = new SparkContext("local", "test")

    val attrIdx = 14
    val query = "TRUCK"
    val baseRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
      .map(_.split('|').toSeq)
    val firstRecord = baseRDD.first()
    val schema = StructType(firstRecord.map(StructField(_, StringType)))
    val attrName = schema(attrIdx).name
    val tableRDD = baseRDD.filter(_ != firstRecord).map(Row.fromSeq(_))
    val succinctTableRDD = SuccinctTableRDD(tableRDD, schema).persist()

    // Compute expected values
    val expectedSearchResults = baseRDD.filter(_(attrIdx).equalsIgnoreCase(query)).map(_.mkString("|"))
      .collect().sorted

    // Compute results
    val searchResults = succinctTableRDD.search(attrName, query.getBytes).map(_.toSeq.map(_.toString).mkString("|"))
      .collect().sorted

    assert(expectedSearchResults === searchResults)
  }

  test("Test count") {
    sc = new SparkContext("local", "test")

    val attrIdx = 14
    val query = "TRUCK"
    val baseRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
      .map(_.split('|').toSeq)
    val firstRecord = baseRDD.first()
    val schema = StructType(firstRecord.map(StructField(_, StringType)))
    val attrName = schema(attrIdx).name
    val tableRDD = baseRDD.filter(_ != firstRecord).map(Row.fromSeq(_))
    val succinctTableRDD = SuccinctTableRDD(tableRDD, schema).persist()

    // Compute expected value
    val expectedCount = baseRDD.filter(_(attrIdx).equalsIgnoreCase(query)).count()

    // Compute result
    val count = succinctTableRDD.count(attrName, query.getBytes)

    assert(count === expectedCount)
  }

  test("Test save and retrieve") {
    sc = new SparkContext("local", "test")

    val baseRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
      .map(_.split('|').toSeq)
    val firstRecord = baseRDD.first()
    val schema = StructType(firstRecord.map(StructField(_, StringType)))
    val tableRDD = baseRDD.filter(_ != firstRecord).map(Row.fromSeq(_))
    val succinctTableRDD = SuccinctTableRDD(tableRDD, schema).persist()

    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct"
    succinctTableRDD.save(succinctDir)

    val originalEntries = succinctTableRDD.collect()
    val newEntries = SuccinctTableRDD(sc, succinctDir).collect()

    assert(originalEntries === newEntries)
  }

}
