package edu.berkeley.cs.succinct.examples

import edu.berkeley.cs.succinct.SuccinctRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Benchmarks search on a Wikipedia dataset provided as an input.
 */
object WikiBench {

  val numRepeats = 10
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

    System.out.println("Benchmarking with words: ")
    words.foreach(System.out.println)

    val wikiData = ctx.textFile(dataPath, partitions).coalesce(partitions).persist(StorageLevel.DISK_ONLY)

    // Ensure all partitions are in memory
    System.out.println("Number of lines = " + wikiData.count())

    System.out.println("Benchmarking Spark RDD...")
    words.foreach(w => {
      var time = 0.0
      var count = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results = wikiData.filter(_.contains(w))
        count += results.count()
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      count = count / numRepeats
      time = time / numRepeats
      System.out.println(s"$w\t$count\t$time")
    })

    wikiData.persist(StorageLevel.MEMORY_ONLY)

    // Ensure all partitions are in memory
    System.out.println("Number of lines = " + wikiData.count())

    System.out.println("Benchmarking Spark RDD...")
    words.foreach(w => {
      var time = 0.0
      var count = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results = wikiData.filter(_.contains(w))
        count += results.count()
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      count = count / numRepeats
      time = time / numRepeats
      System.out.println(s"$w\t$count\t$time")
    })

    val wikiSuccinctData = SuccinctRDD(ctx, succinctDataPath, StorageLevel.MEMORY_ONLY).persist()
    wikiData.unpersist()

    // Ensure all partitions are in memory
    System.out.println("Number of lines = " + wikiSuccinctData.count("\n".getBytes()))

    System.out.println("Benchmarking Succinct RDD search offsets...")
    words.foreach(w => {
      var time = 0.0
      var count = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results = wikiSuccinctData.searchRecords(w.getBytes()).collect()
        count += results.map(_.size).sum
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      count = count / numRepeats
      time = time / numRepeats
      System.out.println(s"$w\t$count\t$time")
    })

    System.out.println("Benchmarking Succinct RDD search records...")
    words.foreach(w => {
      var time = 0.0
      var count = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results = wikiSuccinctData.searchRecords(w.getBytes()).records()
        count += results.count
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      count = count / numRepeats
      time = time / numRepeats
      System.out.println(s"$w\t$count\t$time")
    })

  }

}
