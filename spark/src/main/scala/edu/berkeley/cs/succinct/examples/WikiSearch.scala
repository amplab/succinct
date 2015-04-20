package edu.berkeley.cs.succinct.examples

import edu.berkeley.cs.succinct.SuccinctRDD
import org.apache.spark.{SparkConf, SparkContext}

object WikiSearch {

  def main(args: Array[String]) = {
    val dataPath = args(0)
    val sc = new SparkContext(new SparkConf().setAppName("wiki search"))
    val partitions = if (args.length > 1) args(1).toInt else 2
    val wikiDataRDD = sc.textFile(dataPath, partitions)
      .map(_.getBytes)
    val wikiSuccinctRDD = SuccinctRDD(wikiDataRDD).persist

    // Operations with the term "berkeley"
    val berkeleyCount = wikiSuccinctRDD.count("berkeley".getBytes)
    println("# of times berkeley appears in text = " + berkeleyCount)

    val berkeleyRecords = wikiSuccinctRDD.searchRecords("berkeley".getBytes)
      .records()
      .map(new String(_))
    println("10 records in which berkeley appears: ")
    berkeleyRecords.top(10).foreach(println)

    // Operations with the term "stanford"
    val stanfordCount = wikiSuccinctRDD.count("stanford".getBytes)
    println("# of times stanford appears in text = " + stanfordCount)

    val stanfordRecords = wikiSuccinctRDD.searchRecords("stanford".getBytes)
      .records()
      .map(new String(_))
    println("10 records in which stanford appears: ")
    stanfordRecords.top(10).foreach(println)

    // Regex search operations
    val regexResults = wikiSuccinctRDD.regexSearchRecords("(berkeley|stanford).edu")
      .map(new String(_))
    println("# of records containing the regular expression (berkeley|stanford).edu = " + regexResults.count)

    println("10 records containing the regular expression (berkeley|stanford).edu:")
    regexResults.top(10).foreach(println)

  }
}
