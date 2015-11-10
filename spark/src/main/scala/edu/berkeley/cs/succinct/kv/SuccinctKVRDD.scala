package edu.berkeley.cs.succinct.kv

import java.io.ByteArrayOutputStream

import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.kv.impl.SuccinctKVRDDImpl
import edu.berkeley.cs.succinct.SuccinctCore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

abstract class SuccinctKVRDD[K: ClassTag](
    @transient sc: SparkContext,
    @transient deps: Seq[Dependency[_]])
  extends RDD[(K, Array[Byte])](sc, deps) {

  /**
   * Returns the RDD of partitions.
   *
   * @return The RDD of partitions.
   */
  private[succinct] def partitionsRDD: RDD[SuccinctKVPartition[K]]

  /**
   * Returns first parent of the RDD.
   *
   * @return The first parent of the RDD.
   */
  protected[succinct] def getFirstParent: RDD[SuccinctKVPartition[K]] = {
    firstParent[SuccinctKVPartition[K]]
  }

  /**
   * Returns the array of partitions.
   *
   * @return The array of partitions.
   */
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(K, Array[Byte])] = {
    val succinctIterator = firstParent[SuccinctKVPartition[K]].iterator(split, context)
    if (succinctIterator.hasNext) {
      succinctIterator.next().iterator
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
    partitionsRDD.map(_.count).aggregate(0L)(_ + _, _ + _)
  }

  /**
   * Get the value for a given key.
   *
   * @param key Input key.
   * @return Value corresponding to key.
   */
  def get(key: K): Array[Byte] = {
    val values = partitionsRDD.map(buf => buf.get(key)).filter(v => v != null).collect()
    if (values.length > 1) {
      throw new IllegalStateException(s"Key ${key.toString} returned ${values.length} values")
    }
    if (values.length == 0) null else values(0)
  }

  /**
   * Random access into the value for a given key.
   *
   * @param key Input key.
   * @param offset Offset into value.
   * @param length Number of bytes to be fetched.
   * @return Extracted bytes from the value corresponding to given key.
   */
  def extract(key: K, offset: Int, length: Int): Array[Byte] = {
    val values = partitionsRDD.map(buf => buf.extract(key, offset, length)).filter(v => v != null)
      .collect()
    if (values.length > 1) {
      throw new IllegalStateException(s"Key ${key.toString} returned ${values.length} values")
    }
    if (values.length == 0) null else values(0)
  }

  /**
   * Search for a term across values, and return matched keys.
   *
   * @param query The search term.
   * @return An RDD of matched keys.
   */
  def search(query: Array[Byte]): RDD[K] = {
    partitionsRDD.flatMap(_.search(query))
  }

  /**
   * Search for a term across values, and return matched keys.
   *
   * @param query The search term.
   * @return An RDD of matched keys.
   */
  def search(query: String): RDD[K] = {
    search(query.getBytes("utf-8"))
  }

  /**
   * Search for a term across values, and return the total number of occurrences.
   *
   * @param query The search term.
   * @return Count of the number of occurrences.
   */
  def count(query: Array[Byte]): Long = {
    partitionsRDD.map(_.count(query)).aggregate(0L)(_ + _, _ + _)
  }

  /**
   * Search for a term across values, and return the total number of occurrences.
   *
   * @param query The search term.
   * @return Count of the number of occurrences.
   */
  def count(query: String): Long = {
    count(query.getBytes("utf-8"))
  }

  /**
   * Search for a term across values and return offsets for the matches relative to the beginning of
   * the value. The result is a collection of (key, offset) pairs, where the offset is relative to
   * the beginning of the corresponding value.
   *
   * @param query The search term.
   * @return An RDD of (key, offset) pairs.
   */
  def searchOffsets(query: Array[Byte]): RDD[(K, Int)] = {
    partitionsRDD.flatMap(_.searchOffsets(query))
  }

  /**
   * Search for a term across values and return offsets for the matches relative to the beginning of
   * the value. The result is a collection of (key, offset) pairs, where the offset is relative to
   * the beginning of the corresponding value.
   *
   * @param query The search term.
   * @return An RDD of (key, offset) pairs.
   */
  def searchOffsets(query: String): RDD[(K, Int)] = {
    searchOffsets(query.getBytes("utf-8"))
  }

  /**
   * Search for a regular expression across values, and return matched keys.
   *
   * @param query The regex query.
   * @return An RDD of matched keys.
   */
  def regexSearch(query: String): RDD[K] = {
    partitionsRDD.flatMap(_.regexSearch(query))
  }

  /**
   * Saves the SuccinctKVRDD at the specified path.
   *
   * @param location The path where the SuccinctKVRDD should be stored.
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
  def apply[K: ClassTag](inputRDD: RDD[(K, Array[Byte])])
    (implicit ordering: Ordering[K])
  : SuccinctKVRDD[K] = {
    val partitionsRDD = inputRDD.sortByKey().mapPartitions(createSuccinctKVPartition[K]).cache()
    new SuccinctKVRDDImpl[K](partitionsRDD)
  }

  /**
   * Reads a SuccinctRDD from disk.
   *
   * @param sc The spark context
   * @param location The path to read the SuccinctRDD from.
   * @return The SuccinctRDD.
   */
  def apply[K: ClassTag](sc: SparkContext, location: String, storageLevel: StorageLevel)
      (implicit ordering: Ordering[K])
  : SuccinctKVRDD[K] = {
    val locationPath = new Path(location)
    val fs = FileSystem.get(locationPath.toUri, sc.hadoopConfiguration)
    val status = fs.listStatus(locationPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.startsWith("part-")
      }
    })
    val numPartitions = status.length
    val succinctPartitions = sc.parallelize(0 to numPartitions - 1, numPartitions)
      .mapPartitionsWithIndex[SuccinctKVPartition[K]]((i, partition) => {
        val partitionLocation = location.stripSuffix("/") + "/part-" + "%05d".format(i)
        Iterator(SuccinctKVPartition[K](partitionLocation, storageLevel))
      }).cache()
    new SuccinctKVRDDImpl[K](succinctPartitions)
  }

  /**
   * Creates a [[SuccinctKVPartition]] from a partition of the input RDD.
   *
   * @param kvIter The iterator over the input partition data.
   * @tparam K The type for the keys.
   * @return An iterator over the [[SuccinctKVPartition]]
   */
  private[succinct] def createSuccinctKVPartition[K: ClassTag](kvIter: Iterator[(K, Array[Byte])])
    (implicit ordering: Ordering[K]):
  Iterator[SuccinctKVPartition[K]] = {
    val keysBuffer = new ArrayBuffer[K]()
    var offsetsBuffer = new ArrayBuffer[Int]()
    var buffers = new ArrayBuffer[Array[Byte]]()

    var offset = 0
    var bufferSize = 0

    while (kvIter.hasNext) {
      val curRecord = kvIter.next()
      keysBuffer += curRecord._1
      buffers += curRecord._2
      bufferSize += (curRecord._2.length + 1)
      offsetsBuffer += offset
      offset = bufferSize
    }

    val rawBufferOS = new ByteArrayOutputStream(bufferSize)
    for (i <- buffers.indices) {
      val curRecord = buffers(i)
      rawBufferOS.write(curRecord)
      rawBufferOS.write(SuccinctCore.EOL)
    }

    val keys = keysBuffer.toArray
    val rawValueBuffer = rawBufferOS.toByteArray
    val offsets = offsetsBuffer.toArray
    val valueBuffer = new SuccinctIndexedFileBuffer(rawValueBuffer, offsets)

    Iterator(new SuccinctKVPartition[K](keys, valueBuffer))
  }
}
