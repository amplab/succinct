package edu.berkeley.cs.succinct.examples

import edu.berkeley.cs.succinct.kv.SuccinctKVRDD
import org.apache.spark.{SparkConf, SparkContext}

object KVSearch {

  val searchQuery = "Berkeley"

  val regexQuery = "(Bill|William) Clinton"

  def main(args: Array[String]) = {

    if (args.length < 1) {
      System.err.println("Usage: KVSearch <values-file> <partitions>")
      System.exit(1)
    }

    val dataPath = args(0)
    val partitions = if (args.length > 1) args(1).toInt else 1

    val sparkConf = new SparkConf().setAppName("WikiSearch")
    val ctx = new SparkContext(sparkConf)

    val wikiData = ctx.textFile(dataPath, partitions).map(_.getBytes)
    val wikiKVData = wikiData.zipWithIndex().map(t => (t._2, t._1)).repartition(partitions)

    val succinctKV = SuccinctKVRDD(wikiKVData).persist()

    // Get a particular value
    val value = succinctKV.get(0)
    println("Value corresponding to key 0 = " + new String(value))

    // Random access into a value
    val valueData = succinctKV.extract(0, 1, 3)
    println("Value data for key 0 at offset 1 and length 3 = " + new String(valueData))

    // Search on values
    val keys = succinctKV.search(searchQuery)
    println("First 10 keys matching the search query:")
    keys.take(10).foreach(println)

    // Regex search on values
    val regexKeys = succinctKV.regexSearch(regexQuery)
    println("First 10 keys matching the regex query:")
    regexKeys.take(10).foreach(println)

  }
}
