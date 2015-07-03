package edu.berkeley.cs.succinct.examples

import edu.berkeley.cs.succinct.SuccinctRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Benchmarks search on a Wikipedia dataset provided as an input.
 */
object WikiBench {

  val words = Seq("enactments", "subcostal", "Ellsberg", "chronometer", "lobbed",
    "Reckoning", "Counter-Terrorism", "overpopulated", "retriever", "nosewheel")

  def main(args: Array[String]) = {

    if (args.length < 2) {
      System.err.println("Usage: WikiBench <raw-data> <succinct-data> [<partitions>]")
      System.exit(1)
    }

    val dataPath = args(0)
    val succinctDataPath = args(1)
    val partitions = if (args.length > 2) args(2).toInt else 1

    val sparkConf = new SparkConf().setAppName("WikiBench")
    val ctx = new SparkContext(sparkConf)

    val wikiData = ctx.textFile(dataPath, partitions).coalesce(partitions).persist()

    System.out.println("Benchmarking with words: ")
    words.foreach(System.out.println)

    // Ensure all partitions are in memory
    System.out.println("Number of lines = " + wikiData.count())

    System.out.println("Benchmarking Spark RDD...")
    words.foreach(w => {
      val startTime = System.currentTimeMillis()
      val results = wikiData.filter(_.contains(w))
      val count = results.count()
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      System.out.println(s"$w\t$count\t$totTime")
    })

    val wikiSuccinctData = SuccinctRDD(ctx, succinctDataPath, StorageLevel.MEMORY_ONLY).persist()
    wikiData.unpersist()

    // Ensure all partitions are in memory
    System.out.println("Number of lines = " + wikiSuccinctData.count("\n".getBytes()))

    System.out.println("Benchmarking Succinct RDD search offsets...")
    words.foreach(w => {
      val startTime = System.currentTimeMillis()
      val results = wikiSuccinctData.searchRecords(w.getBytes()).collect()
      val count = results.map(_.size).sum
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      System.out.println(s"$w\t$count\t$totTime")
    })

    System.out.println("Benchmarking Succinct RDD search records...")
    words.foreach(w => {
      val startTime = System.currentTimeMillis()
      val results = wikiSuccinctData.searchRecords(w.getBytes()).records()
      val count = results.map(_.size).sum
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      System.out.println(s"$w\t$count\t$totTime")
    })

  }

}
