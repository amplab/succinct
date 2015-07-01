package edu.berkeley.cs.succinct.examples

import edu.berkeley.cs.succinct.SuccinctRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Performs search on a Wikipedia dataset provided as an input.
 */
object WikiSearch {

  def main(args: Array[String]) = {

    if (args.length < 1) {
      System.err.println("Usage: WikiSearch <file> <partitions>")
      System.exit(1)
    }

    val dataPath = args(0)
    val partitions = if (args.length > 1) args(1).toInt else 1

    val sparkConf = new SparkConf().setAppName("WikiSearch")
    val ctx = new SparkContext(sparkConf)

    val wikiData = ctx.textFile(dataPath, partitions).map(_.getBytes)
    val wikiSuccinctData = SuccinctRDD(wikiData).persist()

    // Operations with the term "berkeley"
    val berkeleyCount = wikiSuccinctData.count("berkeley".getBytes)
    println("# of times berkeley appears in text = " + berkeleyCount)

    val berkeleyRecords = wikiSuccinctData.searchRecords("berkeley".getBytes)
      .records()
      .map(new String(_))
    println("10 articles in which berkeley appears: ")
    berkeleyRecords.top(10).foreach(println)

    // Operations with the term "stanford"
    val stanfordCount = wikiSuccinctData.count("stanford".getBytes)
    println("# of times stanford appears in text = " + stanfordCount)

    val stanfordRecords = wikiSuccinctData.searchRecords("stanford".getBytes)
      .records()
      .map(new String(_))
    println("10 articles in which stanford appears: ")
    stanfordRecords.top(10).foreach(println)

    // Regex search operations (berkeley|stanford).edu
    val regexResults = wikiSuccinctData.regexSearchRecords("(berkeley|stanford).edu")
      .map(new String(_))
    println("# of records containing the regular expression (berkeley|stanford).edu = " + regexResults.count)

    println("10 articles containing the regular expression (berkeley|stanford).edu:")
    regexResults.top(10).foreach(println)

  }
}
