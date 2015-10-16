package edu.berkeley.cs.succinct.examples

import java.io.FileWriter

import edu.berkeley.cs.succinct.SuccinctRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random
import scala.util.parsing.json._

object WikiJSONBench {

  // Constants
  val WARMUP_COUNT: Int = 20
  val MEASURE_COUNT: Int = 100

  // Query data
  var words: Seq[String] = _
  var wordsWarmup: Seq[String] = _
  var wordsMeasure: Seq[String] = _

  // Output path
  var outPath: String = _

  def search(dataStr: String, str: String): Array[Long] = {
    var lastIndex = 0L
    val results: ArrayBuffer[Long] = new ArrayBuffer[Long]()
    while (lastIndex != -1) {
      lastIndex = dataStr.indexOf(str, lastIndex.toInt).toLong
      if (lastIndex != -1) {
        results += lastIndex
        lastIndex += str.length
      }
    }
    results.toArray
  }

  def benchSparkRDD(rdd: RDD[Map[String, String]]): Unit = {
    val storageLevel = rdd.getStorageLevel match {
      case StorageLevel.DISK_ONLY => "disk"
      case StorageLevel.MEMORY_ONLY => "mem"
      case _ => "undf"
    }

    println(s"Benchmarking Spark RDD $storageLevel search offsets...")

    // Warmup
    wordsWarmup.foreach(w => {
      val count = rdd.map(entry => {
        val article = entry.get("article") match {
          case Some(x: String) => x
          case _ => null
        }
        search(article, w)
      }).count()
      println(s"$w\t$count")
    })

    // Measure
    val outSearch = new FileWriter(outPath + "/spark-" + storageLevel + "-search")
    wordsMeasure.foreach(w => {
      val startTime = System.currentTimeMillis()
      val count = rdd.map(entry => {
        val article = entry.get("article") match {
          case Some(x: String) => x
          case _ => null
        }
        search(article, w)
      }).count()
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outSearch.write(s"$w\t$count\t$totTime\n")
    })
    outSearch.close()
  }

  def benchSuccinctRDD(rdd: SuccinctRDD): Unit = {
    println("Benchmarking Succinct RDD search offsets...")

    // Warmup
    wordsWarmup.foreach(w => {
      val count = rdd.searchOffsets(w).count()
      println(s"$w\t$count")
    })

    // Measure
    val outSearch = new FileWriter(outPath + "/succinct-search")
    wordsMeasure.foreach(w => {
      val startTime = System.currentTimeMillis()
      val count = rdd.searchOffsets(w).count()
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outSearch.write(s"$w\t$count\t$totTime\n")
    })
    outSearch.close()
  }

  def sampleSeq[T](input: Seq[T], sampleSize: Int): Seq[T] = {
    Seq.fill(sampleSize)(input(Random.nextInt(input.length)))
  }

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: WikiBench <json-data> <succinct-data> <partitions> <query-path> <output-path>")
      System.exit(1)
    }

    val dataPath = args(0)
    val succinctPath = args(1)
    val partitions = args(2).toInt
    val queryPath = args(3)
    outPath = args(4)

    // Create queries
    words = Source.fromFile(queryPath).getLines().toSeq
    wordsWarmup = sampleSeq(words, WARMUP_COUNT)
    wordsMeasure = sampleSeq(words, MEASURE_COUNT)

    val sparkConf = new SparkConf().setAppName("WikiJSONBench")
    val ctx = new SparkContext(sparkConf)

    val jsonRDD = ctx.textFile(dataPath).map(line => {
      JSON.parseFull(line) match {
        case Some(e: Map[String,String]) => e
        case _ => null
      }
    }).repartition(partitions)

    jsonRDD.persist(StorageLevel.DISK_ONLY)
    println("count = " + jsonRDD.count())
    benchSparkRDD(jsonRDD)
    jsonRDD.unpersist()

    jsonRDD.persist(StorageLevel.MEMORY_ONLY)
    println("count = " + jsonRDD.count())
    benchSparkRDD(jsonRDD)
    jsonRDD.unpersist()

    val succinctRDD = SuccinctRDD(ctx, succinctPath, StorageLevel.MEMORY_ONLY).cache()
    benchSuccinctRDD(succinctRDD)
    succinctRDD.unpersist()
  }
}
