package edu.berkeley.cs.succinct.kv

import java.io.ByteArrayOutputStream

import edu.berkeley.cs.succinct.buffers.SuccinctKVBuffer
import edu.berkeley.cs.succinct.kv.impl.SuccinctKVRDDImpl
import edu.berkeley.cs.succinct.streams.SuccinctKVStream
import edu.berkeley.cs.succinct.{SuccinctCore, SuccinctKV}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

abstract class SuccinctKVRDD[K <: Comparable[K] : ClassTag](@transient sc: SparkContext,
                                                            @transient deps: Seq[Dependency[_]])
  extends RDD[(K, Array[Byte])](sc, deps) {

  /**
   * Returns the RDD of partitions.
   *
   * @return The RDD of partitions.
   */
  private[succinct] def partitionsRDD: RDD[SuccinctKV[K]]

  /**
   * Returns first parent of the RDD.
   *
   * @return The first parent of the RDD.
   */
  protected[succinct] def getFirstParent: RDD[SuccinctKV[K]] = {
    firstParent[SuccinctKV[K]]
  }

  /**
   * Returns the array of partitions.
   *
   * @return The array of partitions.
   */
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(K, Array[Byte])] = {
    val succinctIterator = firstParent[SuccinctKV[K]].iterator(split, context)
    if (succinctIterator.hasNext) {
      new SuccinctKVIterator(succinctIterator.next())
    } else {
      Iterator[(K, Array[Byte])]()
    }
  }

  /**
   * Count the number of KV-pairs in the SuccinctKVRDD.
   *
   * @return The number of KV-pairs in the SuccinctKVRDD.
   */
  override def count(): Long = {
    partitionsRDD.map(buf => buf.numEntries()).aggregate(0L)(_ + _, _ + _)
  }

  /**
   * Get the value for a given key.
   *
   * @param key Input key.
   * @return Value corresponding to key.
   */
  def get(key: K): Array[Byte] = {
    val values = partitionsRDD.map(buf => buf.get(key)).filter(v => (v != null)).collect()
    if (values.size > 1) {
      throw new IllegalStateException("Single key should not return multiple values")
    }
    if (values.size == 0) null else values(0)
  }

  /**
   * Get delete the entry corresponding to a key.
   *
   * @param key Input key.
   * @return True if delete is successful, false otherwise.
   */
  def delete(key: K): Boolean = {
    partitionsRDD.map(_.delete(key)).aggregate(false)(_ | _, _ | _)
  }

  /**
   * Saves the SuccinctRDD at the specified path.
   *
   * @param location The path where the SuccinctRDD should be stored.
   */
  def save(location: String): Unit = {
    val path = new Path(location)
    val fs = FileSystem.get(path.toUri, new Configuration())
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    }

    partitionsRDD.zipWithIndex().foreach(entry => {
      val i = entry._2
      val partition = entry._1
      val partitionLocation = location.stripSuffix("/") + "/part-" + "%05d".format(i)
      val path = new Path(partitionLocation)
      val fs = FileSystem.get(path.toUri, new Configuration())
      val os = fs.create(path)
      partition.writeToStream(os)
      os.close()
    })

    val successPath = new Path(location.stripSuffix("/") + "/_SUCCESS")
    fs.create(successPath).close()
  }
}

/** Factory for [[SuccinctKVRDD]] instances **/
object SuccinctKVRDD {

  /**
   * Converts an input RDD to [[SuccinctKVRDD]].
   *
   * @param inputRDD The input RDD.
   * @tparam K The key type.
   * @return The SuccinctKVRDD.
   */
  def apply[K <: Comparable[K] : ClassTag](inputRDD: RDD[(K, Array[Byte])]): SuccinctKVRDD[K] = {
    val partitionsRDD = inputRDD.sortByKey().mapPartitions(createSuccinctKVBuffer[K])
    new SuccinctKVRDDImpl[K](partitionsRDD.cache())
  }

  /**
   * Reads a SuccinctRDD from disk.
   *
   * @param sc The spark context
   * @param location The path to read the SuccinctRDD from.
   * @return The SuccinctRDD.
   */
  def apply[K <: Comparable[K] : ClassTag](sc: SparkContext, location: String, storageLevel: StorageLevel): SuccinctKVRDD[K] = {
    val locationPath = new Path(location)
    val fs = FileSystem.get(locationPath.toUri, sc.hadoopConfiguration)
    val status = fs.listStatus(locationPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.startsWith("part-")
      }
    })
    val numPartitions = status.length
    val succinctPartitions = sc.parallelize(0 to numPartitions - 1, numPartitions)
      .mapPartitionsWithIndex[SuccinctKV[K]]((i, partition) => {
      val partitionLocation = location.stripSuffix("/") + "/part-" + "%05d".format(i)
      val path = new Path(partitionLocation)
      val fs = FileSystem.get(path.toUri, new Configuration())
      val is = fs.open(path)
      val partitionIterator = storageLevel match {
        case StorageLevel.MEMORY_ONLY =>
          Iterator(new SuccinctKVBuffer[K](is))
        case StorageLevel.DISK_ONLY =>
          Iterator(new SuccinctKVStream[K](path))
        case _ =>
          Iterator(new SuccinctKVBuffer[K](is))
      }
      is.close()
      partitionIterator
    })
    new SuccinctKVRDDImpl[K](succinctPartitions.cache())
  }

  /**
   * Creates a [[SuccinctKV]] from a partition of the input RDD.
   *
   * @param kvIter The iterator over the input partition data.
   * @tparam K The type for the keys.
   * @return An iterator over the [[SuccinctKV]]
   */
  private[succinct] def createSuccinctKVBuffer[K <: Comparable[K] : ClassTag](kvIter: Iterator[(K, Array[Byte])]): Iterator[SuccinctKV[K]] = {
    val keysBuffer = new java.util.ArrayList[K]()
    var offsets = new ArrayBuffer[Int]()
    var buffers = new ArrayBuffer[Array[Byte]]()

    var offset = 0
    var bufferSize = 0

    while (kvIter.hasNext) {
      val curRecord = kvIter.next()
      keysBuffer.add(curRecord._1)
      buffers += curRecord._2
      bufferSize += (curRecord._2.size + 1)
      offsets += offset
      offset = bufferSize
    }

    val rawBufferOS = new ByteArrayOutputStream(bufferSize)
    for (i <- 0 to buffers.size - 1) {
      val curRecord = buffers(i)
      rawBufferOS.write(curRecord)
      rawBufferOS.write(SuccinctCore.EOL)
    }

    Iterator(new SuccinctKVBuffer[K](keysBuffer, rawBufferOS.toByteArray, offsets.toArray))
  }
}
