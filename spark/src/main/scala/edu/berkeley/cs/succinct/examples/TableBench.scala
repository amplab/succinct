package edu.berkeley.cs.succinct.examples

import java.io.FileWriter

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, DataFrame}

import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Random

object TableBench {

  // Queries
  var queries: Array[String] = _
  var warmupQueries: Array[String] = _
  var measureQueries: Array[String] = _

  // Constants
  val WARMUP_COUNT: Int = 20

  // Output path
  var outPath: String = _

  def sampleArr[T: ClassTag](input: Array[T], sampleSize: Int): Array[T] = {
    Array.fill(sampleSize)(input(Random.nextInt(input.length)))
  }

  def filterQuery(query: String, df: DataFrame): DataFrame = {
    df.filter(query)
  }

  def benchDF(df: DataFrame, dfType: String) = {
    println(s"Benchmarking DataFrame $dfType...")

    // Warmup
    warmupQueries.foreach(k => {
      val length = filterQuery(k, df).count()
      println(s"$k\t$length")
    })

    // Measure
    val outFilter = new FileWriter(outPath + "/df-" + dfType)
    measureQueries.foreach(k => {
      val startTime = System.currentTimeMillis()
      val length = filterQuery(k, df).count()
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outFilter.write(s"$k\t$length\t$totTime\n")
    })
    outFilter.close()
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TableBench <parquet-data> <succinct-data> <query-path> <output-path>")
      System.exit(1)
    }

    val parquetDataPath = args(0)
    val succinctDataPath = args(1)
    val queryPath = args(2)
    outPath = args(3)

    queries = Source.fromFile(queryPath)
      .getLines()
      .map(line => line.split('\t'))
      .map(lineArr => "col" + lineArr(0) + "=\"" + lineArr(1) + "\"")
      .toArray

    warmupQueries = sampleArr(queries, WARMUP_COUNT)
    measureQueries = queries

    val sqlCtx = new SQLContext(new SparkContext(new SparkConf().setAppName("TableBench")))

    val parquetDF = sqlCtx.read.parquet(parquetDataPath).cache()
    benchDF(parquetDF, "parquet")

    val succinctDF = sqlCtx.read.format("edu.berkeley.cs.succinct.sql").load(succinctDataPath)
    benchDF(succinctDF, "succinct")
  }

}
