package edu.berkeley.cs.succinct.examples

import java.io.FileWriter

import edu.berkeley.cs.succinct.SuccinctRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random
import scala.util.matching.Regex

/**
 * Benchmarks search on a Wikipedia dataset provided as an input.
 */
object WikiBench {

  // Metadata for executing Spark RDD queries
  var partitionOffsets: Seq[Long] = _
  var partitionSizes: Seq[Long] = _

  // Query data
  var words: Seq[String] = _
  var regex: Seq[String] = _
  var offsets: Seq[Long] = _

  var wordsWarmup: Seq[String] = _
  var wordsMeasure: Seq[String] = _
  var offsetsWarmup: Seq[Long] = _
  var offsetsMeasure: Seq[Long] = _
  val extractLen: Int = 1024

  // Constants
  val WARMUP_COUNT: Int = 20
  val MEASURE_COUNT: Int = 100

  // Output path
  var outPath: String = _

  def sampleSeq[T](input: Seq[T], sampleSize: Int): Seq[T] = {
    Seq.fill(sampleSize)(input(Random.nextInt(input.length)))
  }

  def count(data: Array[Byte], str: String): Long = {
    var lastIndex = 0L
    var result: Long = 0
    val dataStr = new String(data)
    while (lastIndex != -1) {
      lastIndex = dataStr.indexOf(str, lastIndex.toInt).toLong
      if (lastIndex != -1) {
        result += 1
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

  def regexSearch(it: Iterator[Array[Byte]], partitionOffset: Long, re: String): Map[Long, Int] = {
    var curOffset = partitionOffset
    val rex = new Regex(re)
    var rec: String = null
    var results: Map[Long, Int] = new TreeMap[Long, Int]()
    while (it.hasNext) {
      rec = new String(it.next())
      results ++= rex.findAllMatchIn(rec).map(m => (curOffset + m.start, m.end - m.start)).toMap
      curOffset += (rec.length + 1L)
    }
    results
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
    while (ret.length < length && it.hasNext) {
      // Fetch the next record
      rec = it.next()
      ret = ret ++ rec.slice(0, length - ret.length)
    }
    ret
  }

  def countRDD(rdd: RDD[Array[Byte]], str: String): Long = {
    rdd.map(count(_, str)).aggregate(0L)(_ + _, _ + _)
  }

  def searchRDD(rdd: RDD[Array[Byte]], partitionOffsets: Seq[Long], str: String): RDD[Long] = {
    rdd.mapPartitionsWithIndex((idx, it) => {
      val res = search(it, partitionOffsets(idx), str)
      Iterator(res)
    }).flatMap(_.iterator)
  }

  def regexSearchRDD(rdd: RDD[Array[Byte]], partitionOffsets: Seq[Long], re: String): RDD[(Long, Int)] = {
    rdd.mapPartitionsWithIndex((idx, it) => {
      val res = regexSearch(it, partitionOffsets(idx), re)
      res.iterator
    })
  }

  def extractRDD(rdd: RDD[Array[Byte]], partitionOffsets: Seq[Long], partitionSizes: Seq[Long],
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

  def benchSparkRDD(rdd: RDD[Array[Byte]]): Unit = {
    val storageLevel = rdd.getStorageLevel match {
      case StorageLevel.DISK_ONLY => "disk"
      case StorageLevel.MEMORY_ONLY => "mem"
      case _ => "undf"
    }

    println(s"Benchmarking Spark RDD $storageLevel count offsets...")

    // Warmup
    wordsWarmup.foreach(w => {
      val count = countRDD(rdd, w)
      println(s"$w\t$count")
    })

    // Measure
    val outCount = new FileWriter(outPath + "/spark-" + storageLevel + "-count")
    wordsMeasure.foreach(w => {
      val startTime = System.currentTimeMillis()
      val count = countRDD(rdd, w)
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outCount.write(s"$w\t$count\t$totTime\n")
    })
    outCount.close()

    println(s"Benchmarking Spark RDD $storageLevel search offsets...")

    // Warmup
    wordsWarmup.foreach(w => {
      val count = searchRDD(rdd, partitionOffsets, w).count()
      println(s"$w\t$count")
    })

    // Measure
    val outSearch = new FileWriter(outPath + "/spark-" + storageLevel + "-search")
    wordsMeasure.foreach(w => {
      val startTime = System.currentTimeMillis()
      val count = searchRDD(rdd, partitionOffsets, w).count()
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outSearch.write(s"$w\t$count\t$totTime\n")
    })
    outSearch.close()

    println(s"Benchmarking Spark RDD $storageLevel random access...")

    // Warmup
    offsetsWarmup.foreach(o => {
      val length = extractRDD(rdd, partitionOffsets, partitionSizes, o, extractLen).length
      println(s"$o\t$length")
    })

    // Measure
    val outExtract = new FileWriter(outPath + "/spark-" + storageLevel + "-extract")
    offsetsMeasure.foreach(o => {
      val startTime = System.currentTimeMillis()
      val length = extractRDD(rdd, partitionOffsets, partitionSizes, o, extractLen).length
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outExtract.write(s"$o\t$length\t$totTime\n")
    })
    outExtract.close()
  }

  def benchSuccinctRDD(rdd: SuccinctRDD): Unit = {
    println("Benchmarking Succinct RDD count offsets...")

    // Warmup
    wordsWarmup.foreach(w => {
      val count = rdd.countOffsets(w)
      println(s"$w\t$count")
    })

    // Measure
    val outCount = new FileWriter(outPath + "/succinct-count")
    wordsMeasure.foreach(w => {
      val startTime = System.currentTimeMillis()
      val count = rdd.countOffsets(w)
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outCount.write(s"$w\t$count\t$totTime\n")
    })
    outCount.close()

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

    println("Benchmarking Succinct RDD random access...")
    offsetsWarmup.foreach(o => {
      val length = rdd.extract(o, extractLen).length
      println(s"$o\t$length")
    })

    // Measure
    val outExtract = new FileWriter(outPath + "/succinct-extract")
    offsetsMeasure.foreach(o => {
      val startTime = System.currentTimeMillis()
      val length = rdd.extract(o, extractLen).length
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outExtract.write(s"$o\t$length\t$totTime\n")
    })
    outExtract.close()

  }

  def main(args: Array[String]) : Unit = {

    if (args.length < 5) {
      System.err.println("Usage: WikiBench <raw-data> <succinct-data> <partitions> <query-path> <output-path>")
      System.exit(1)
    }

    val dataPath = args(0)
    val succinctDataPath = args(1)
    val partitions = args(2).toInt
    val queryPath = args(3)
    outPath = args(4)

    val sparkConf = new SparkConf().setAppName("WikiBench")
    val ctx = new SparkContext(sparkConf)

    words = Source.fromFile(queryPath).getLines().toSeq

    // Create RDD
    val wikiDataDisk = ctx.textFile(dataPath, partitions).map(_.getBytes).repartition(partitions).persist(StorageLevel.DISK_ONLY)

    // Compute partition sizes and partition offsets
    partitionSizes = wikiDataDisk.mapPartitionsWithIndex((idx, partition) => {
        val partitionSize = partition.aggregate(0L)((sum, record) => sum + (record.length + 1), _ + _)
        Iterator((idx, partitionSize))
      }).collect.sorted.map(_._2).toSeq
    partitionOffsets = partitionSizes.scanLeft(0L)(_ + _).slice(0, partitionSizes.size)

    offsets = Random.shuffle(partitionOffsets.zip(partitionSizes)
      .map(range => (0 to 99).map(i => range._1 + (Math.abs(Random.nextLong()) % (range._2 - extractLen))))
      .flatMap(_.iterator).toList).take(10)

    // Create queries
    wordsWarmup = sampleSeq(words, WARMUP_COUNT)
    wordsMeasure = sampleSeq(words, MEASURE_COUNT)
    offsetsWarmup = sampleSeq(offsets, WARMUP_COUNT)
    offsetsMeasure = sampleSeq(offsets, MEASURE_COUNT)

    // Benchmark DISK_ONLY
    benchSparkRDD(wikiDataDisk)
    wikiDataDisk.unpersist()

    val wikiDataMem = ctx.textFile(dataPath, partitions).map(_.getBytes).repartition(partitions).persist(StorageLevel.MEMORY_ONLY)

    // Ensure all partitions are in memory
    println("Number of lines = " + wikiDataMem.count())

    // Benchmark MEMORY_ONLY
    benchSparkRDD(wikiDataMem)
    wikiDataMem.unpersist()

    val wikiSuccinctData = SuccinctRDD(ctx, succinctDataPath, StorageLevel.MEMORY_ONLY).persist()

    // Ensure all partitions are in memory
    println("Number of lines = " + wikiSuccinctData.countOffsets("\n".getBytes()))

    // Benchmark Succinct
    benchSuccinctRDD(wikiSuccinctData)
    wikiSuccinctData.unpersist()

  }

}
