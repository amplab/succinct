package edu.berkeley.cs.succinct

import java.io.ByteArrayOutputStream

import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.impl.SuccinctRDDImpl
import edu.berkeley.cs.succinct.streams.SuccinctIndexedFileStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Extends `RDD[Array[Byte]]` to a SuccinctRDD, which stores each partition as a SuccinctIndexedBuffer.
 * SuccinctRDD supports count, search and extractPerPartition operations, which treat each partition as a flat file.
 * SuccinctRDD additionally supports countRecord, searchRecord and extractRecord operations, which perform
 * count, search and extract operations on a record granularity.
 */

abstract class SuccinctRDD(@transient sc: SparkContext,
    @transient deps: Seq[Dependency[_]])
  extends RDD[Array[Byte]](sc, deps) {

  /**
   * Returns the RDD of partitions.
   *
   * @return The RDD of partitions.
   */
  private[succinct] def partitionsRDD: RDD[SuccinctIndexedFile]

  /**
   * Returns first parent of the RDD.
   *
   * @return The first parent of the RDD.
   */
  protected[succinct] def getFirstParent: RDD[SuccinctIndexedFile] = {
    firstParent[SuccinctIndexedFile]
  }

  /**
   * Returns the array of partitions.
   *
   * @return The array of partitions.
   */
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /** Overrides the compute function to return a SuccinctIterator. */
  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val succinctIterator = firstParent[SuccinctIndexedFile].iterator(split, context)
    if (succinctIterator.hasNext) {
      new SuccinctIterator(succinctIterator.next())
    } else {
      Iterator[Array[Byte]]()
    }
  }

  /**
   * Search for all occurrences of a query within the RDD.
   *
   * @param query The search query.
   * @return The RDD of iterables over offsets into each partition.
   */
  def searchOffsets(query: Array[Byte]): RDD[Long] = {
    partitionsRDD.map(buf => buf.search(query).map(Long2long).toIterable).flatMap(_.iterator)
  }

  /**
   * Search for all occurrences of a query string within the RDD.
   *
   * @param query The search query.
   * @return The RDD of offsets.
   */
  def searchOffsets(query: String): RDD[Long] = {
    searchOffsets(query.getBytes("utf-8"))
  }

  /**
   * Counts for all occurrences of a query in the RDD.
   *
   * @param query The count query.
   * @return The count of the number of occurrences of the query.
   */
  def countOffsets(query: Array[Byte]): Long = {
    partitionsRDD.map(buf => buf.count(query)).aggregate(0L)(_ + _, _ + _)
  }

  /**
   * Counts for all occurrences of a query in the RDD.
   *
   * @param query The count query.
   * @return The count of the number of occurrences of the query.
   */
  def countOffsets(query: String): Long = {
    countOffsets(query.getBytes("utf-8"))
  }

  def extract(offset: Long, length: Int): Array[Byte]

  /**
   * Extracts data from each partition for a given offset and length.
   *
   * @param offset Offset into the partitions.
   * @param length Length of data to be extracted.
   * @return RDD of the extracted data.
   */
  def extractPerPartition(offset: Int, length: Int): RDD[Array[Byte]] = {
    partitionsRDD.map(buf => buf.extract(offset + buf.getFileOffset, length))
  }

  /**
   * Entry corresponding to each matched pattern for a given regex query; the entry
   * encapsulates the pattern offset and length.
   *
   * @constructor Creates a PatternEntry from the offset and length.
   * @param offset The offset corresponding to the pattern.
   * @param length The length of the matched pattern.
   */
  class PatternEntry(offset: java.lang.Long, length: java.lang.Integer) {
    /**
     * Get the pattern offset.
     *
     * @return The pattern offset.
     */
    def patternOffset: Long = offset

    /**
     * Get the pattern length.
     *
     * @return The pattern length.
     */
    def patternLength: Integer = length
  }

  /**
   * Searches for the input regular expression within each RDD and
   * returns results as (offset, length) pairs relative to each partition.
   * The query must be UTF-8 encoded.
   *
   * @param query The regular expression search query.
   * @return RDD of an iterable over matched pattern occurrences.
   */
  def regexSearchOffsets(query: String): RDD[Iterable[PatternEntry]] = {
    partitionsRDD.map(buf => buf.regexSearch(query).toMap.map(t => new PatternEntry(t._1, t._2)))
  }

  /**
   * Searches for all records that match a query and
   * returns results as offsets relative to each partition.
   *
   * @param query The search query.
   * @return The SearchOffsetResultsRDD corresponding to the search query.
   */
  def search(query: Array[Byte]): SearchOffsetResultsRDD = {
    new SearchOffsetResultsRDD(this, query)
  }

  /**
   * Searches for all records that match a query and
   * returns results as offsets relative to each partition.
   *
   * @param query The search query.
   * @return The SearchOffsetResultsRDD corresponding to the search query.
   */
  def search(query: String): SearchOffsetResultsRDD = {
    search(query.getBytes("utf-8"))
  }

  /**
   * Counts for all occurrences of records that match a query.
   *
   * @param query The count query.
   * @return The count of the number of occurrences of the count query.
   */
  def count(query: Array[Byte]): Long = {
    partitionsRDD.map(buf => buf.recordCount(query)).aggregate(0L)(_ + _, _ + _)
  }

  /**
   * Counts for all occurrences of records that match a query.
   *
   * @param query The count query.
   * @return The count of the number of occurrences of the count query.
   */
  def count(query: String): Long = {
    count(query.getBytes("utf-8"))
  }

  /**
   * Extracts data from each record for a given offset and length.
   *
   * @param offset The offset into each record.
   * @param length The length of the data to be extracted.
   * @return The ExtractResultsRDD corresponding to the query.
   */
  def extractRecords(offset: Int, length: Int): ExtractResultsRDD = {
    new ExtractResultsRDD(this, offset, length)
  }

  /**
   * Searches of all records that contains a regular expression search
   * query and returns all such records.
   *
   * @param query The regular expression search query.
   * @return The RDD containing all records that match the regular expression search.
   */
  def regexSearch(query: String): RDD[Array[Byte]] = {
    partitionsRDD.map(buf => buf.recordSearchRegex(query)).flatMap(_.iterator)
  }

  /**
   * Count the number of records in the SuccinctRDD.
   *
   * @return The number of records in the SuccinctRDD.
   */
  override def count(): Long = {
    partitionsRDD.map(buf => buf.getNumRecords).aggregate(0L)(_ + _, _ + _)
  }

  /**
   * Saves the SuccinctRDD at the specified path.
   *
   * @param location The path where the SuccinctRDD should be stored.
   */
  def save(location: String): Unit = {
    val path = new Path(location)
    val fs = FileSystem.get(path.toUri, new Configuration())
    if(!fs.exists(path)) {
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

/** Factory for [[SuccinctRDD]] instances */
object SuccinctRDD {

  /**
   * Converts an input RDD to SuccinctRDD.
   *
   * @param inputRDD The input RDD.
   * @return The SuccinctRDD.
   */
  def apply(inputRDD: RDD[Array[Byte]]): SuccinctRDD = {
    val partitionSizes = inputRDD.mapPartitionsWithIndex((idx, partition) =>
      {
        val partitionSize = partition.aggregate(0L)((sum, record) => sum + (record.length + 1), _ + _)
        Iterator((idx, partitionSize))
      }
    ).collect.sorted.map(_._2)
    val partitionOffsets = partitionSizes.scanLeft(0L)(_ + _)
    partitionOffsets.foreach(println)
    val succinctPartitions = inputRDD.mapPartitionsWithIndex((i, p) =>
      createSuccinctBuffer(partitionOffsets(i), p))
    new SuccinctRDDImpl(succinctPartitions)
  }

  /**
   * Reads a SuccinctRDD from disk.
   *
   * @param sc The spark context
   * @param location The path to read the SuccinctRDD from.
   * @return The SuccinctRDD.
   */
  def apply(sc: SparkContext, location: String, storageLevel: StorageLevel): SuccinctRDD = {
    val locationPath = new Path(location)
    val fs = FileSystem.get(locationPath.toUri, sc.hadoopConfiguration)
    val status = fs.listStatus(locationPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.startsWith("part-")
      }
    })
    val numPartitions = status.length
    val succinctPartitions = sc.parallelize(0 to numPartitions - 1, numPartitions)
      .mapPartitionsWithIndex[SuccinctIndexedFile]((i, partition) => {
        val partitionLocation = location.stripSuffix("/") + "/part-" + "%05d".format(i)
        val path = new Path(partitionLocation)
        val fs = FileSystem.get(path.toUri, new Configuration())
        val is = fs.open(path)
        val partitionIterator = storageLevel match {
          case StorageLevel.MEMORY_ONLY =>
            Iterator(new SuccinctIndexedFileBuffer(is))
          case StorageLevel.DISK_ONLY =>
            Iterator(new SuccinctIndexedFileStream(path))
          case _ =>
            Iterator(new SuccinctIndexedFileBuffer(is))
        }
        is.close()
        partitionIterator
      })
    new SuccinctRDDImpl(succinctPartitions)
  }

  /**
   * Creates a SuccinctIndexedBuffer from a partition of the input RDD.
   *
   * @param dataIter The iterator over the input partition data.
   * @return An Iterator over the SuccinctIndexedBuffer.
   */
  private[succinct] def createSuccinctBuffer(partitionOffset: Long, dataIter: Iterator[Array[Byte]]):
      Iterator[SuccinctIndexedFile] = {
    var offsets = new ArrayBuffer[Int]()
    var buffers = new ArrayBuffer[Array[Byte]]()
    var offset = 0
    var partitionSize = 0
    while (dataIter.hasNext) {
      val curRecord = dataIter.next()
      buffers += curRecord
      partitionSize += (curRecord.size + 1)
      offsets += offset
      offset += (curRecord.length + 1)
    }

    val rawBufferOS = new ByteArrayOutputStream(partitionSize)
    for (i <- 0 to buffers.size - 1) {
      val curRecord = buffers(i)
      rawBufferOS.write(curRecord)
      rawBufferOS.write(SuccinctCore.EOL)
    }

    val ret = Iterator(new SuccinctIndexedFileBuffer(rawBufferOS.toByteArray, offsets.toArray, partitionOffset))
    ret
  }

}
