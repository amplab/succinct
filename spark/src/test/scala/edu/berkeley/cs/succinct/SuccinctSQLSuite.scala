package edu.berkeley.cs.succinct.sql

import java.io.{IOException, File}

import com.google.common.io.Files
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.util.Random

private[succinct] object TestUtils {

  /**
   * This function deletes a file or a directory with everything that's in it. This function is
   * copied from Spark with minor modifications made to it. See original source at:
   * github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/Utils.scala
   */
  def deleteRecursively(file: File) {
    def listFilesSafely(file: File): Seq[File] = {
      if (file.exists()) {
        val files = file.listFiles()
        if (files == null) {
          throw new IOException("Failed to list files for dir: " + file)
        }
        files
      } else {
        List()
      }
    }

    if (file != null) {
      try {
        if (file.isDirectory) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }
}

class SuccinctSQLSuite extends FunSuite {
  val rawTable = getClass.getResource("/table.dat").getFile
  val succinctTable = getClass.getResource("/table.succinct").getFile
  val citiesTable = getClass.getResource("/cities.dat").getFile

  test("dsl test") {
    val results = TestSQLContext
      .succinctFile(succinctTable)
      .select("shipmode")
      .collect()

    assert(results.size === 1000)
  }

  test("sql test") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE succinctTable
         |USING edu.berkeley.cs.succinct.sql
         |OPTIONS (path "$succinctTable")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT * FROM succinctTable").collect().size === 1000)
  }

  test("Convert specific SparkSQL types to succinct") {
    val testSchema = StructType(Seq(
      StructField("Name", StringType, false),
      StructField("Length", IntegerType, true),
      StructField("Area", DoubleType, false),
      StructField("Airport", BooleanType, true)))

    val cityRDD = sparkContext.parallelize(Seq(
      Row("San Francisco", 12, 44.5, true),
      Row("Palo Alto", 12, 22.3, false),
      Row("Munich", 8, 3.14, true)))
    val cityDataFrame = TestSQLContext.createDataFrame(cityRDD, testSchema)

    val tempDir = Files.createTempDir()
    val succinctDir = tempDir + "/succinct"
    cityDataFrame.saveAsSuccinctFiles(succinctDir)

    assert(TestSQLContext.succinctFile(succinctDir).collect().size == 3)

    val cities = TestSQLContext
      .succinctFile(succinctDir)
      .select("Name")
      .collect()
    assert(cities.map(_(0)).toSet == Set("San Francisco", "Palo Alto", "Munich"))

    val lengths = TestSQLContext
      .succinctFile(succinctDir)
      .select("Length")
      .collect()
    assert(lengths.map(_(0)).toSet == Set(12, 12, 8))

    val airports = TestSQLContext
      .succinctFile(succinctDir)
      .select("Airport")
      .collect()
    assert(airports.map(_(0)).toSeq == Seq(true, false, true))
  }

  test("prune") {
    val testSchema = StructType(Seq(
      StructField("Name", StringType, false),
      StructField("Length", IntegerType, true),
      StructField("Area", DoubleType, false),
      StructField("Airport", BooleanType, true)))

    val cityRDD = sparkContext.textFile(citiesTable)
      .map(_.split(','))
      .map(t => Row(t(0), t(1).toInt, t(2).toDouble, t(3).toBoolean))

    val cityDataFrame = TestSQLContext.createDataFrame(cityRDD, testSchema)

    val tempDir = Files.createTempDir()
    val succinctDir = tempDir + "/succinct"
    cityDataFrame.saveAsSuccinctFiles(succinctDir)

    val loadedDF = TestSQLContext
      .succinctFile(succinctDir)

    val cities = loadedDF
      .select("Name")
      .collect()
    assert(cities.length == 385)

  }

  test("filters") {
    val testSchema = StructType(Seq(
      StructField("Name", StringType, false),
      StructField("Length", IntegerType, true),
      StructField("Area", DoubleType, false),
      StructField("Airport", BooleanType, true)))

    val cityRDD = sparkContext.textFile(citiesTable)
      .map(_.split(','))
      .map(t => Row(t(0), t(1).toInt, t(2).toDouble, t(3).toBoolean))

    val cityDataFrame = TestSQLContext.createDataFrame(cityRDD, testSchema)

    val tempDir = Files.createTempDir()
    val succinctDir = tempDir + "/succinct"
    cityDataFrame.saveAsSuccinctFiles(succinctDir)
    val loadedDF = TestSQLContext.succinctFile(succinctDir)

    def checkFilters[T](column: String, makeThresholds: => Seq[T]) = {
      def check(column: String, op: String, threshold: T) = {
        val expected = cityDataFrame.filter(s"$column $op $threshold")
        val actual = loadedDF.filter(s"$column $op $threshold")
        assert(actual.count() === expected.count(), s"fails $op $threshold on column $column")
      }
      for (threshold <- makeThresholds) {
        for (op <- Seq("<", "<=", ">", ">=", "=")) {
          check(column, op, threshold)
        }
      }
    }

    val rand = new Random()
    checkFilters("Name", Seq("''", "'Z'", "'Las Vegas'", "'Aberdeen'", "'Bronxville'"))
    checkFilters("Length", Seq.fill(2)(rand.nextInt(1000)))
    checkFilters("Area", Seq(-1, 0.0, 999.2929, 1618.15, 9, 659))
    checkFilters("Airport", Seq(false, true))
  }

  test("test load and save") {
    // Test if load works as expected
    val df = TestSQLContext.load(succinctTable, "edu.berkeley.cs.succinct.sql")
    assert(df.count == 1000)

    // Test if save works as expected
    val tempSaveDir = Files.createTempDir().getAbsolutePath
    TestUtils.deleteRecursively(new File(tempSaveDir))
    df.save(tempSaveDir, "edu.berkeley.cs.succinct.sql")
    val newDf = TestSQLContext.load(tempSaveDir, "edu.berkeley.cs.succinct.sql")
    assert(newDf.count == 1000)
  }

}
