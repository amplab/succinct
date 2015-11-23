package edu.berkeley.cs.succinct.examples

import edu.berkeley.cs.succinct.json._
import org.apache.spark.{SparkConf, SparkContext}

object JsonSearch {

  val fieldName = "location.city"
  val fieldValue = "Berkeley"
  val searchQuery = "Cookie"

  def main(args: Array[String]) = {

    if (args.length < 1) {
      System.err.println("Usage: JsonSearch <values-file> <partitions>")
      System.exit(1)
    }

    val dataPath = args(0)
    val partitions = if (args.length > 1) args(1).toInt else 1

    val sparkConf = new SparkConf().setAppName("WikiSearch")
    val ctx = new SparkContext(sparkConf)

    val jsonData = ctx.textFile(dataPath, partitions)

    val succinctJsonRDD = jsonData.succinctJson

    // Get a particular JSON document
    val value = succinctJsonRDD.get(0)
    println("Value corresponding to Ids 0 = " + new String(value))

    // Search across JSON Documents
    val ids1 = succinctJsonRDD.search(searchQuery)
    println("Ids matching the search query:")
    ids1.foreach(println)

    // Filter on attributes
    val ids2 = succinctJsonRDD.filter(fieldName, fieldValue)
    println("Ids matching the filter query:")
    ids2.foreach(println)

  }
}
