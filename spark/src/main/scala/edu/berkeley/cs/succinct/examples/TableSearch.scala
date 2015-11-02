package edu.berkeley.cs.succinct.examples

import com.google.common.io.Files
import edu.berkeley.cs.succinct.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Performs search on a TPC-H dataset provided as an input.
 */
object TableSearch {
  def main(args: Array[String]) = {

    if (args.length < 1) {
      System.err.println("Usage: TableSearch <csv-file> <partitions>")
      System.exit(1)
    }

    val dataPath = args(0)
    val ctx = new SparkContext(new SparkConf().setAppName("TableSearch"))
    val sqlCtx = new SQLContext(ctx)
    val partitions = if (args.length > 1) args(1).toInt else 1
    val csvData = ctx.textFile(dataPath)
      .map(_.split('|').toSeq)
    val firstRecord = csvData.first()
    val schema = StructType(firstRecord.map(StructField(_, StringType)))
    val tableRDD = csvData.filter(_ != firstRecord).map(Row.fromSeq(_)).repartition(partitions)

    val dataFrame = sqlCtx.createDataFrame(tableRDD, schema)
    val tempDir = Files.createTempDir()
    val succinctDir = tempDir + "/succinct"
    dataFrame.write.format("edu.berkeley.cs.succinct.sql").save(succinctDir)

    val succinctDataFrame = sqlCtx.succinctTable(succinctDir)

    // Search for "TRUCK" for attribute shipmode
    val attrName = "shipmode"
    val query = "TRUCK"

    // Get search results
    val records = succinctDataFrame.filter(s"$attrName = \'$query\'")
    println("10 records matching the query:")
    records.take(10).foreach(println)

  }
}
