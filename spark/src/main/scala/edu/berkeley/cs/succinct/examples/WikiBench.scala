package edu.berkeley.cs.succinct.examples

import edu.berkeley.cs.succinct.SuccinctRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Benchmarks search on a Wikipedia dataset provided as an input.
 */
object WikiBench {

  val numRepeats = 10
  val words = Seq("enactments", "subcostal", "Ellsberg", "chronometer", "lobbed",
    "Reckoning", "Counter-Terrorism", "overpopulated", "retriever", "nosewheel")
  val randoms = (0 to 99).map(t => Math.abs(Random.nextLong()))
  val extractLen = 1024

  def count(data: Array[Byte], str: String): Long = {
    var lastIndex = 0L
    var result: Long = 0
    val dataStr = new String(data)
    while (lastIndex != -1) {
      result += 1
      lastIndex = dataStr.indexOf(str, lastIndex.toInt).toLong
      if (lastIndex != -1) {
        lastIndex += str.length
      }
    }
    result
  }

  def search(data: Array[Byte], str: String): Array[Long] = {
    var lastIndex = 0L
    val dataStr = new String(data)
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

  def search(it: Iterator[Array[Byte]], partitionOffset: Long, str: String): Array[Long] = {
    var curOffset = partitionOffset
    var rec: Array[Byte] = null
    val results: ArrayBuffer[Long] = new ArrayBuffer[Long]()
    while (it.hasNext) {
      rec = it.next()
      results ++= search(rec, str).map(_ + curOffset)
      curOffset += (rec.length + 1)
    }
    results.toArray
  }

  def extract(it: Iterator[Array[Byte]], partitionOffset: Long, offset: Long, length: Int): Array[Byte] = {
    var curOffset = partitionOffset
    var rec: Array[Byte] = null
    while(curOffset <= offset && it.hasNext) {
      rec = it.next()
      curOffset += (rec.length + 1)
    }

    if (curOffset <= offset) {
      throw new ArrayIndexOutOfBoundsException("Invalid offset = " + offset)
    }

    // Roll back to the beginning of last record
    curOffset -= (rec.length + 1)
    // Compute offset into record
    val recordOffset = (offset - curOffset).toInt
    // Compute the record slice
    var ret = rec.slice(recordOffset, recordOffset + length)
    // If there are insufficient number of bytes in this record,
    // Fetch from next record
    if (ret.length < length && it.hasNext) {
      // Fetch the next record
      rec = it.next()
      ret = ret ++ rec.slice(0, length - ret.length)
    }
    ret
  }

  def countRDD(rdd: RDD[Array[Byte]], str: String): Long = {
    rdd.map(count(_, str)).aggregate(0L)(_ + _, _ + _)
  }

  def searchRDD(rdd: RDD[Array[Byte]], partitionOffsets: Array[Long], str: String): RDD[Long] = {
    rdd.mapPartitionsWithIndex((idx, it) => {
      val res = search(it, partitionOffsets(idx), str)
      Iterator(res)
    }).flatMap(_.iterator)
  }

  def extractRDD(rdd: RDD[Array[Byte]], partitionOffsets: Array[Long], partitionSizes: Array[Long],
                 offset: Long, length: Int): Array[Byte] = {
    val extractResults = rdd.mapPartitionsWithIndex((idx, it) => {
      val offBeg = partitionOffsets(idx)
      val offEnd = offBeg + partitionSizes(idx)
      if(offset >= offBeg && offset < offEnd) {
        val res = extract(it, partitionOffsets(idx), offset, length)
        Iterator(res)
      } else {
        Iterator()
      }
    }).collect
    if (extractResults.size != 1) {
      throw new ArrayIndexOutOfBoundsException("Invalid output; size = " + extractResults.size
        + "; values = " + extractResults.mkString(",") + "; offset = " + offset)
    }
    extractResults(0)
  }

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

    // Create RDD
    val wikiData = ctx.textFile(dataPath, partitions).map(_.getBytes).repartition(partitions).persist(StorageLevel.DISK_ONLY)

    // Compute partition sizes and partition offsets
    val partitionSizes = wikiData.mapPartitionsWithIndex((idx, partition) =>
      {
        val partitionSize = partition.aggregate(0L)((sum, record) => sum + (record.length + 1), _ + _)
        Iterator((idx, partitionSize))
      }).collect.sorted.map(_._2)
    val partitionOffsets = partitionSizes.scanLeft(0L)(_ + _).slice(0, partitionSizes.size)
    val dataSize = partitionSizes.sum
    val offsets = randoms.map(_ % dataSize)

    // Benchmark DISK_ONLY
    println("Benchmarking Spark RDD count offsets (DISK_ONLY)...")
    words.foreach(w => {
      var time = 0.0
      var count = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results = countRDD(wikiData, w)
        count += results
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      count = count / numRepeats
      time = time / numRepeats
      println(s"$w\t$count\t$time")
    })

    println("Benchmarking Spark RDD search offsets (DISK_ONLY)...")
    words.foreach(w => {
      var time = 0.0
      var count = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results = searchRDD(wikiData, partitionOffsets, w)
        count += results.count()
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      count = count / numRepeats
      time = time / numRepeats
      println(s"$w\t$count\t$time")
    })

    println("Benchmarking Spark RDD random access (DISK_ONLY)...")
    offsets.foreach(o => {
      var time = 0.0
      var length = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results = extractRDD(wikiData, partitionOffsets, partitionSizes, o, extractLen)
        length += results.length
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      length = length / numRepeats
      time = time / numRepeats
      println(s"$o\t$length\t$time")
    })

    wikiData.persist(StorageLevel.MEMORY_ONLY)

    // Ensure all partitions are in memory
    println("Number of lines = " + wikiData.count())

    // Benchmark MEMORY_ONLY
    println("Benchmarking Spark RDD count offsets (MEMORY_ONLY)...")
    words.foreach(w => {
      var time = 0.0
      var count = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results = countRDD(wikiData, w)
        count += results
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      count = count / numRepeats
      time = time / numRepeats
      println(s"$w\t$count\t$time")
    })

    println("Benchmarking Spark RDD search offsets (MEMORY_ONLY)...")
    words.foreach(w => {
      var time = 0.0
      var count = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results = searchRDD(wikiData, partitionOffsets, w)
        count += results.count()
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      count = count / numRepeats
      time = time / numRepeats
      println(s"$w\t$count\t$time")
    })

    println("Benchmarking Spark RDD random access (MEMORY_ONLY)...")
    offsets.foreach(o => {
      var time = 0.0
      var length = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results = extractRDD(wikiData, partitionOffsets, partitionSizes, o, extractLen)
        length += results.length
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      length = length / numRepeats
      time = time / numRepeats
      println(s"$o\t$length\t$time")
    })

    val wikiSuccinctData = SuccinctRDD(ctx, succinctDataPath, StorageLevel.MEMORY_ONLY).persist()
    wikiData.unpersist()

    // Ensure all partitions are in memory
    println("Number of lines = " + wikiSuccinctData.countOffsets("\n".getBytes()))

    // Benchmark Succinct
    println("Benchmarking Succinct RDD count offsets...")
    words.foreach(w => {
      var time = 0.0
      var count = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results = wikiSuccinctData.countOffsets(w)
        count += results
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      count = count / numRepeats
      time = time / numRepeats
      println(s"$w\t$count\t$time")
    })

    println("Benchmarking Succinct RDD search offsets...")
    words.foreach(w => {
      var time = 0.0
      var count = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results = wikiSuccinctData.searchOffsets(w).collect()
        count += results.size
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      count = count / numRepeats
      time = time / numRepeats
      println(s"$w\t$count\t$time")
    })

    println("Benchmarking Succinct RDD random access...")
    offsets.foreach(o => {
      var time = 0.0
      var length = 0.0
      for (i <- 1 to numRepeats) {
        val startTime = System.currentTimeMillis()
        val results =  wikiSuccinctData.extract(o, extractLen)
        length += results.length
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        time += totTime
      }
      length = length / numRepeats
      time = time / numRepeats
      println(s"$o\t$length\t$time")
    })

  }

}
