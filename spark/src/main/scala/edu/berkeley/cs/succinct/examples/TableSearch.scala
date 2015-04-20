package edu.berkeley.cs.succinct.examples

import edu.berkeley.cs.succinct.sql.SuccinctTableRDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object TableSearch {
  def main(args: Array[String]) = {
    val dataPath = args(0)
    val sc = new SparkContext(new SparkConf().setAppName("table search"))
    val sqlContext = new SQLContext(sc)
    val partitions = if (args.length > 1) args(1).toInt else 2
    val baseRDD = sc.textFile(dataPath, partitions)
      .map(_.split('|').toSeq)
    val firstRecord = baseRDD.first()
    val schema = StructType(firstRecord.map(StructField(_, StringType)))
    val tableRDD = baseRDD.filter(_ != firstRecord).map(Row.fromSeq(_))
    val succinctTableRDD = SuccinctTableRDD(tableRDD, schema).persist

    // Search for "TRUCK" for attribute shipmode
    val attrName = "shipmode"
    val query = "TRUCK"

    // Get the count
    val count = succinctTableRDD.count(attrName, query.getBytes)
    println(s"Count of TRUCK in attribute=$attrName = $count")

    // Get search results
    val records = succinctTableRDD.search(attrName, query.getBytes)
    println("Records matching the query:")
    records.foreach(println)

  }
}
