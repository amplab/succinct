package edu.berkeley.cs.succinct.examples

import edu.berkeley.cs.succinct._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Performs search on a Wikipedia dataset provided as an input.
 */
object WikiSearch {

  val extractOffset = 0
  val extractLength = 100
  val searchQuery = "Berkeley"
  val regexQuery = "(Bill|William) Clinton"

  def main(args: Array[String]) = {

    if (args.length < 1) {
      System.err.println("Usage: WikiSearch <file> <partitions>")
      System.exit(1)
    }

    val dataPath = args(0)
    val partitions = if (args.length > 1) args(1).toInt else 1

    val sparkConf = new SparkConf().setAppName("WikiSearch")
    val ctx = new SparkContext(sparkConf)

    val wikiData = ctx.textFile(dataPath).map(_.getBytes).repartition(partitions)
    val wikiSuccinctData = wikiData.succinct

    // Count the number of occurrences of searchQuery in text
    val count = wikiSuccinctData.count(searchQuery)
    println(s"# of times $searchQuery appears in text = " + count)

    // Find all offsets of occurrences of searchQuery in text
    val searchOffsets = wikiSuccinctData.search(searchQuery)
    println(s"First 10 locations in RDD where $searchQuery occurs: ")
    searchOffsets.take(10).foreach(println)

    // Regex search operation
    val regexResults = wikiSuccinctData.regexSearch(regexQuery)
    println(s"# of occurrences of the regular expression $regexQuery = " + regexResults.count)

    println(s"10 occurrences of the regular expression $regexQuery:")
    regexResults.top(10).foreach(println)

  }
}
