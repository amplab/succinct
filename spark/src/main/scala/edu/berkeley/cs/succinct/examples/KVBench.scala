package edu.berkeley.cs.succinct.examples

import java.io.FileWriter

import edu.berkeley.cs.succinct.kv.SuccinctKVRDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.util.Random

object KVBench {

  // Constants
  val WARMUP_COUNT: Int = 20
  val MEASURE_COUNT: Int = 100

  // Queries
  var keys: Array[java.lang.Long] = _
  var keysWarmup: Array[java.lang.Long] = _
  var keysMeasure: Array[java.lang.Long] = _

  // Output path
  var outPath: String = _

  def sampleArr[T: ClassTag](input: Array[T], sampleSize: Int): Array[T] = {
    Array.fill(sampleSize)(input(Random.nextInt(input.length)))
  }

  def get(rdd: RDD[(java.lang.Long, Array[Byte])], key: java.lang.Long): Array[Byte] = {
    val res = rdd.filter(kv => kv._1 == key).collect()
    if (res.size == 0) {
      throw new ArrayIndexOutOfBoundsException(s"Invalid key = $key")
    }
    if (res.size > 1) {
      throw new IllegalArgumentException(s"Got ${res.size} values for key = $key")
    }
    res(0)._2
  }

  def benchSparkRDD(rdd: RDD[(java.lang.Long, Array[Byte])]): Unit = {
    val storageLevel = rdd.getStorageLevel match {
      case StorageLevel.DISK_ONLY => "disk"
      case StorageLevel.MEMORY_ONLY => "mem"
      case _ => "undf"
    }

    println(s"Benchmarking Spark RDD $storageLevel get...")

    // Warmup
    keysWarmup.foreach(k => {
      val length = get(rdd, k).length
      println(s"$k\t$length")
    })

    // Measure
    val outGet = new FileWriter(outPath + "/spark-" + storageLevel + "-get")
    keysMeasure.foreach(k => {
      val startTime = System.currentTimeMillis()
      val length = get(rdd, k).length
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outGet.write(s"$k\t$length\t$totTime\n")
    })
    outGet.close()
  }

  def benchSuccinctRDD(rdd: SuccinctKVRDD[java.lang.Long]): Unit = {
    println("Benchmarking Succinct RDD get...")

    println("Benchmarking Succinct RDD random access...")
    keysWarmup.foreach(k => {
      val length = rdd.get(k).length
      println(s"$k\t$length")
    })

    // Measure
    val outGet = new FileWriter(outPath + "/succinct-get")
    keysMeasure.foreach(k => {
      val startTime = System.currentTimeMillis()
      val length = rdd.get(k).length
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outGet.write(s"$k\t$length\t$totTime\n")
    })
    outGet.close()

  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KVBench <raw-data> <succinct-data> <partitions> <output-path>")
      System.exit(1)
    }

    val dataPath = args(0)
    val succinctDataPath = args(1)
    val partitions = args(2).toInt
    outPath = args(3)

    val sparkConf = new SparkConf().setAppName("KVBench")
    val ctx = new SparkContext(sparkConf)

    val kvRDD = ctx.textFile(dataPath)
      .zipWithIndex
      .map(t => (t._2.asInstanceOf[java.lang.Long], t._1.getBytes))
      .repartition(partitions)

    val kvRDDDisk = kvRDD.persist(StorageLevel.DISK_ONLY)
    val count = kvRDDDisk.count()
    println("Number of entries = " + kvRDDDisk.count())

    keys = Random.shuffle((0 to 9999)
      .map(i => (Math.abs(Random.nextLong()) % count).asInstanceOf[java.lang.Long]))
      .toArray

    // Create queries
    keysWarmup = sampleArr(keys, WARMUP_COUNT)
    keysMeasure = sampleArr(keys, MEASURE_COUNT)

    benchSparkRDD(kvRDDDisk)
    kvRDDDisk.unpersist(true)

    val kvRDDMem = kvRDD.persist(StorageLevel.MEMORY_ONLY)
    println("Number of entries = " + kvRDDMem.count())

    benchSparkRDD(kvRDDMem)
    kvRDDMem.unpersist(true)

    val kvRDDSuccinct = SuccinctKVRDD[java.lang.Long](ctx, succinctDataPath, StorageLevel.MEMORY_ONLY).cache()
    println("Number of entries = " + kvRDDSuccinct.count())

    benchSuccinctRDD(kvRDDSuccinct)
  }
}
