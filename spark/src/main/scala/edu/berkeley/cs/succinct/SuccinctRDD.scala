package edu.berkeley.cs.succinct

import edu.berkeley.cs.succinct.impl.SuccinctRDDImpl
import org.apache.spark._
import org.apache.spark.rdd.RDD

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
  private[succinct] def partitionsRDD: RDD[SuccinctIndexedBuffer]

  /**
   * Returns first parent of the RDD.
   *
   * @return The first parent of the RDD.
   */
  protected[succinct] def getFirstParent: RDD[SuccinctIndexedBuffer] = {
    firstParent[SuccinctIndexedBuffer]
  }

  /** Overrides the compute function to return a SuccinctIterator. */
  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val succinctIterator = firstParent[SuccinctBuffer].iterator(split, context)
    if (succinctIterator.hasNext) {
      new SuccinctIterator(succinctIterator.next())
    } else {
      Iterator[Array[Byte]]()
    }
  }

  /**
   * Search for all occurrences of a query within each partition and
   * returns results as offsets relative to each partition.
   *
   * @param query The search query.
   * @return The RDD of iterables over offsets into each partition.
   */
  def search(query: Array[Byte]): RDD[Iterable[Long]] = {
    partitionsRDD.map(buf => buf.search(query).map(Long2long).toIterable)
  }

  /**
   * Counts for all occurrences of a query in the RDD.
   *
   * @param query The count query.
   * @return The count of the number of occurrences of the query.
   */
  def count(query: Array[Byte]): Long = {
    partitionsRDD.map(buf => buf.count(query)).aggregate(0L)(_ + _, _ + _)
  }

  /**
   * Extracts data from each partition for a given offset and length.
   *
   * @param offset Offset into the partitions.
   * @param length Length of data to be extracted.
   * @return RDD of the extracted data.
   */
  def extractPerPartition(offset: Int, length: Int): RDD[Array[Byte]] = {
    partitionsRDD.map(buf => buf.extract(offset, length))
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
    def patternOffset: Long = offset

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
  def regexSearch(query: String): RDD[Iterable[PatternEntry]] = {
    partitionsRDD.map(buf => buf.regexSearch(query).toMap.map(t => new PatternEntry(t._1, t._2)))
  }

  /**
   * Searches for all records that match a query and
   * returns results as offsets relative to each partition.
   *
   * @param query The search query.
   * @return The SearchOffsetResultsRDD corresponding to the search query.
   */
  def searchRecords(query: Array[Byte]): SearchOffsetResultsRDD = {
    new SearchOffsetResultsRDD(this, query)
  }

  /**
   * Counts for all occurrences of records that match a query.
   *
   * @param query The count query.
   * @return The count of the number of occurrences of the count query.
   */
  def countRecords(query: Array[Byte]): Long = {
    partitionsRDD.map(buf => buf.recordCount(query)).aggregate(0L)(_ + _, _ + _)
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
  def regexSearchRecords(query: String): RDD[Array[Byte]] = {
    partitionsRDD.map(buf => buf.recordSearchRegex(query)).flatMap(_.iterator)
  }

  /**
   * Returns the array of partitions.
   *
   * @return The array of partitions.
   */
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions
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
    val succinctPartitions = inputRDD.mapPartitions(createSuccinctBuffer)
    new SuccinctRDDImpl(succinctPartitions)
  }

  /**
   * Creates a SuccinctIndexedBuffer from a partition of the input RDD.
   *
   * @param dataIter The iterator over the input partition data.
   * @return An Iterator over the SuccinctIndexedBuffer.
   */
  private[succinct] def createSuccinctBuffer(dataIter: Iterator[Array[Byte]]): Iterator[SuccinctIndexedBuffer] = {
    var offsets = new ArrayBuffer[Long]()
    val rawBufferBuilder = new StringBuilder
    var offset = 0
    while (dataIter.hasNext) {
      val curRecord = dataIter.next()
      rawBufferBuilder.append(new String(curRecord))
      rawBufferBuilder.append(SuccinctIndexedBuffer.getRecordDelim.toChar)
      offsets += offset
      offset += (curRecord.length + 1)
    }
    val ret = Iterator(new SuccinctIndexedBuffer(rawBufferBuilder.toString().getBytes, offsets.toArray))
    ret
  }

}
