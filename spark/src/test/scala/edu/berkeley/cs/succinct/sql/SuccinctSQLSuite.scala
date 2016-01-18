package edu.berkeley.cs.succinct.sql

import java.io.{File, IOException}

import com.google.common.io.Files
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.scalatest._

import scala.util.Random

private[succinct] object TestUtils {

  /**
   * This function deletes a file or a directory with everything that's in it.
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

  private[succinct] def castToType(elem: String, dataType: DataType): Any = {
    if (elem == "NULL") return null
    dataType match {
      case BooleanType => elem.equals("1")
      case ByteType => elem.toByte
      case ShortType => elem.toShort
      case IntegerType => elem.toInt
      case LongType => elem.toLong
      case FloatType => elem.toFloat
      case DoubleType => elem.toDouble
      case _: DecimalType => Decimal(new java.math.BigDecimal(elem))
      case StringType => elem
      case other => throw new IllegalArgumentException(s"Unexpected type $dataType.")
    }
  }
}

class SuccinctSQLSuite extends FunSuite with BeforeAndAfterAll {
  val rawTable = getClass.getResource("/table.dat").getFile
  val succinctTable = rawTable + ".succinct"

  val citiesTable = getClass.getResource("/cities.dat").getFile
  val testSchema = StructType(Seq(
    StructField("Name", StringType, nullable = false),
    StructField("Length", IntegerType, nullable = true),
    StructField("Area", DoubleType, nullable = false),
    StructField("Airport", BooleanType, nullable = true)))

  private var sqlContext: SQLContext = _
  private var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext = new SparkContext("local[2]", "Succinct")
    sqlContext = new SQLContext(sparkContext)

    val baseRDD = sqlContext.sparkContext.textFile(getClass.getResource("/table.dat").getFile)
      .map(_.split('|').toSeq)
    val firstRecord = baseRDD.first()
    val schema = StructType(firstRecord.map(StructField(_, StringType)))
    val tableRDD = baseRDD.filter(_ != firstRecord).map(Row.fromSeq(_))
    val succinctTableRDD = SuccinctTableRDD(tableRDD, schema).persist()
    succinctTableRDD.save(succinctTable)
  }

  override def afterAll(): Unit = {
    TestUtils.deleteRecursively(new File(succinctTable))
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  def createTestDF(schema: StructType = testSchema): (DataFrame, DataFrame) = {
    val cityRDD = sparkContext.textFile(citiesTable)
      .map(_.split(','))
      .map { t =>
      Row.fromSeq(Seq.tabulate(schema.size)(i => TestUtils.castToType(t(i), schema(i).dataType)))
    }
    val df = sqlContext.createDataFrame(cityRDD, schema)

    val tempDir = Files.createTempDir()
    val succinctDir = tempDir + "/succinct"
    df.saveAsSuccinctTable(succinctDir)
    val loadedDF = sqlContext.succinctTable(succinctDir)
    (df, loadedDF) // (expected, actual: succinct loaded)
  }

  test("dsl test") {
    val results = sqlContext
      .succinctTable(succinctTable)
      .select("shipmode")
      .collect()

    assert(results.length === 1000)
  }

  test("sql test") {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE succinctTable
         |USING edu.berkeley.cs.succinct.sql
         |OPTIONS (path "$succinctTable")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT * FROM succinctTable").collect().length === 1000)
  }

  test("Convert specific SparkSQL types to succinct") {
    val testSchema = StructType(Seq(
      StructField("Name", StringType, nullable = false),
      StructField("Length", IntegerType, nullable = true),
      StructField("Area", DoubleType, nullable = false),
      StructField("Airport", BooleanType, nullable = true)))

    val cityRDD = sparkContext.parallelize(Seq(
      Row("San Francisco", 12, 44.52, true),
      Row("Palo Alto", 12, 22.33, false),
      Row("Munich", 8, 3.14, true)))
    val cityDataFrame = sqlContext.createDataFrame(cityRDD, testSchema)

    val tempDir = Files.createTempDir()
    val succinctDir = tempDir + "/succinct"
    cityDataFrame.saveAsSuccinctTable(succinctDir)

    assert(sqlContext.succinctTable(succinctDir).collect().length == 3)

    val cities = sqlContext
      .succinctTable(succinctDir)
      .select("Name")
      .collect()
    assert(cities.map(_(0)).toSet === Set("San Francisco", "Palo Alto", "Munich"))

    val lengths = sqlContext
      .succinctTable(succinctDir)
      .select("Length")
      .collect()
    assert(lengths.map(_(0)).toSet === Set(12, 12, 8))

    val areas = sqlContext
      .succinctTable(succinctDir)
      .select("Area")
      .collect()
    assert(areas.map(_(0)).toSet === Set(44.52, 22.33, 3.14))

    val airports = sqlContext
      .succinctTable(succinctDir)
      .select("Airport")
      .collect()
    assert(airports.map(_(0)).toSet === Set(true, false, true))
  }

  test("prunes") {
    val (cityDataFrame, loadedDF) = createTestDF(testSchema)

    def checkPrunes(columns: String*) = {
      val expected = cityDataFrame.select(columns.map(cityDataFrame(_)): _*).collect()
      val actual = loadedDF.select(columns.map(loadedDF(_)): _*).collect()
      assert(actual.length === expected.length)
      expected.foreach(row => assert(row.toSeq.length == columns.length))
    }

    checkPrunes("Name")
    checkPrunes("Length")
    checkPrunes("Area")
    checkPrunes("Airport")
    checkPrunes("Name", "Length")
    checkPrunes("Area", "Airport")
    checkPrunes("Name", "Area", "Airport")
    checkPrunes("Name", "Length", "Area", "Airport")
  }

  test("filters") {
    def checkFilters[T](expectedDF: DataFrame, actualDF: DataFrame,
                        column: String, makeThresholds: => Seq[T]) = {
      def check(column: String, op: String, threshold: T) = {
        try {
          val expected = expectedDF.filter(s"$column $op $threshold")
          val actual = actualDF.filter(s"$column $op $threshold")
          assert(actual.count() === expected.count(), s"fails $op $threshold on column $column")
        } catch {
          case e: Exception =>
            println(s"****query: '$column $op $threshold'")
            throw e
        }
      }
      for (threshold <- makeThresholds) {
        for (op <- Seq("<", "<=", ">", ">=", "=")) {
          check(column, op, threshold)
        }
      }
    }

    val rand = new Random()

    // string, integer, double, boolean columns
    val (cityDataFrame, loadedDF) = createTestDF(testSchema)

    checkFilters(cityDataFrame, loadedDF, "Name",
      Seq("''", "'Z'", "'Las Vegas'", "'Aberdeen'", "'Bronxville'"))
    checkFilters(cityDataFrame, loadedDF, "Length",
      Seq.fill(2)(rand.nextInt(1000)))
    checkFilters(cityDataFrame, loadedDF, "Area",
      Seq(-1, 0.0, 999.2929, 1618.15, 9, 659) ++ Seq.fill(2)(rand.nextDouble() * 1000))
    checkFilters(cityDataFrame, loadedDF, "Airport",
      Seq(false, true))

    // parse Area as float column
    val testSchema2 = StructType(Seq(
      StructField("Name", StringType, nullable = false),
      StructField("Length", IntegerType, nullable = true),
      StructField("Area", FloatType, nullable = false), // changed to FloatType
      StructField("Airport", BooleanType, nullable = true)))
    val (cityDataFrame2, loadedDF2) = createTestDF(testSchema2)

    checkFilters(cityDataFrame2, loadedDF2, "Area",
      Seq(-1, 0.0, 999.2929, 1618.15, 9, 659) ++ Seq.fill(2)(rand.nextFloat() * 1000))

    // parse Area as double column
    val testSchema3 = StructType(Seq(
      StructField("Name", StringType, nullable = false),
      StructField("Length", IntegerType, nullable = true),
      StructField("Area", DoubleType, nullable = false), // changed to DoubleType
      StructField("Airport", BooleanType, nullable = true)))
    val (cityDataFrame3, loadedDF3) = createTestDF(testSchema3)

    checkFilters(cityDataFrame3, loadedDF3, "Area",
      Seq(-1, 0.0, 999.2929, 1618.15, 9, 659) ++ Seq.fill(2)(rand.nextDouble() * 1000))

    // parse Length as short column
    val testSchema4 = StructType(Seq(
      StructField("Name", StringType, nullable = false),
      StructField("Length", ShortType, nullable = true), // Changed to ShortType
      StructField("Area", DoubleType, nullable = false),
      StructField("Airport", BooleanType, nullable = true)))
    val (cityDataFrame4, loadedDF4) = createTestDF(testSchema4)

    checkFilters(cityDataFrame4, loadedDF4, "Length",
      Seq.fill(2)(rand.nextInt(1000)))

    // parse Length as long column
    val testSchema5 = StructType(Seq(
      StructField("Name", StringType, nullable = false),
      StructField("Length", LongType, nullable = true), // Changed to ShortType
      StructField("Area", DoubleType, nullable = false),
      StructField("Airport", BooleanType, nullable = true)))
    val (cityDataFrame5, loadedDF5) = createTestDF(testSchema5)

    checkFilters(cityDataFrame5, loadedDF5, "Length",
      Seq.fill(2)(rand.nextInt(1000)))

  }

  test("test load and save") {
    // Test if load works as expected
    val df = sqlContext.read.format("edu.berkeley.cs.succinct.sql").load(succinctTable)
    assert(df.count == 1000)

    // Test if save works as expected
    val tempSaveDir = Files.createTempDir().getAbsolutePath
    TestUtils.deleteRecursively(new File(tempSaveDir))
    df.write.format("edu.berkeley.cs.succinct.sql").save(tempSaveDir)
    val newDf = sqlContext.read.format("edu.berkeley.cs.succinct.sql").load(tempSaveDir)
    assert(newDf.count == 1000)
  }

}
