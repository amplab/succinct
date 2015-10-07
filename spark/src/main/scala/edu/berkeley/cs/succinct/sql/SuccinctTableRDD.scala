package edu.berkeley.cs.succinct.sql

import java.io.ByteArrayOutputStream

import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.sql.impl.SuccinctTableRDDImpl
import edu.berkeley.cs.succinct.streams.SuccinctIndexedFileStream
import edu.berkeley.cs.succinct.{SuccinctCore, SuccinctIndexedFile}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Dependency, Partition, SparkContext}

import scala.Array._
import scala.collection.mutable.ArrayBuffer

/**
 * Extends `RDD[Row]` to a [[SuccinctTableRDD]], which stores each partition as a [[SuccinctIndexedFile]].
 * [[SuccinctTableRDD]] exposes a table interface, allowing search and count operations on any
 * column based on a matching pattern.
 *
 */

abstract class SuccinctTableRDD(@transient sc: SparkContext,
                                @transient deps: Seq[Dependency[_]])
  extends RDD[Row](sc, deps) {

  private[succinct] def partitionsRDD: RDD[SuccinctIndexedFile]

  /**
   * Returns the RDD of partitions.
   *
   * @return The RDD of partitions.
   */
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
   * Returns first parent of the RDD.
   *
   * @return The first parent of the RDD.
   */
  protected[succinct] def getFirstParent: RDD[SuccinctIndexedFile] = {
    firstParent[SuccinctIndexedFile]
  }

  /**
   * Saves the [[SuccinctIndexedFile]] partitions to disk by serializing them.
   *
   * @param path Path to save the serialized partitions to.
   */
  def save(path: String): Unit

  /**
   * Search for all occurrences of a particular attribute value.
   *
   * @param attribute Name of the attribute.
   * @param query The search query.
   * @return An RDD of matching rows.
   */
  def search(attribute: String, query: Array[Byte]): RDD[Row]

  /**
   * Perform a prefix search for all occurrences of a particular substring.
   *
   * @param attribute Name of the attribute.
   * @param query The search query.
   * @return An RDD of matching rows.
   */
  def prefixSearch(attribute: String, query: Array[Byte]): RDD[Row]

  /**
   * Perform a suffix search for all occurrences of a particular substring.
   *
   * @param attribute Name of the attribute.
   * @param query The search query.
   * @return An RDD of matching rows.
   */
  def suffixSearch(attribute: String, query: Array[Byte]): RDD[Row]

  /**
   * Perform a search for all occurrences of a particular substring.
   *
   * @param attribute Name of the attribute.
   * @param query The search query.
   * @return An RDD of matching rows.
   */
  def unboundedSearch(attribute: String, query: Array[Byte]): RDD[Row]

  /**
   * Perform a range search for occurrences lying between two values.
   *
   * @param attribute Name of the attribute.
   * @param queryBegin The beginning of range.
   * @param queryEnd The end of range.
   * @return An RDD of matching rows.
   */
  def rangeSearch(attribute: String, queryBegin: Array[Byte], queryEnd: Array[Byte]): RDD[Row]

  /**
   * Search and extract based on a set of filters and the required columns.
   *
   * @param requiredColumns List of required columns
   * @param filters Set of filters.
   * @return An RDD of matching rows with pruned columns; contains false positives.
   */
  def pruneAndFilter(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row]

  /**
   * Count all occurrences of a particular attribute value.
   *
   * @param attribute Name of the attribute.
   * @param query The count query.
   * @return The count of matching rows.
   */
  def count(attribute: String, query: Array[Byte]): Long
}

/** Factory for [[SuccinctTableRDD]] instances */
object SuccinctTableRDD {

  def apply(sparkContext: SparkContext, path: String, storageLevel: StorageLevel): SuccinctTableRDD = {
    val dataPath = path.stripSuffix("/") + "/data"
    val schemaPath = path.stripSuffix("/") + "/schema"
    val separatorsPath = path.stripSuffix("/") + "/separators"
    val minPath = path.stripSuffix("/") + "/min"
    val maxPath = path.stripSuffix("/") + "/max"
    val conf = new Configuration()
    val succinctDataPath = new Path(dataPath)
    val fs = FileSystem.get(succinctDataPath.toUri, conf)
    val status = fs.listStatus(succinctDataPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.startsWith("part-")
      }
    })
    val numPartitions = status.length
    val succinctPartitions = sparkContext.parallelize((0 to numPartitions - 1), numPartitions)
      .mapPartitionsWithIndex[SuccinctIndexedFile]((i, partition) => {
      val partitionLocation = dataPath + "/part-" + "%05d".format(i)
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
    val succinctSchema: StructType = SuccinctUtils.readObjectFromFS[StructType](conf, schemaPath)
    val succinctSeparators: Array[Byte] = SuccinctUtils.readObjectFromFS[Array[Byte]](conf, separatorsPath)
    val minRow: Row = SuccinctUtils.readObjectFromFS[Row](conf, minPath)
    val maxRow: Row = SuccinctUtils.readObjectFromFS[Row](conf, maxPath)
    val limits = getLimits(maxRow, minRow)
    val succinctSerializer = new SuccinctSerializer(succinctSchema, succinctSeparators, limits)
    new SuccinctTableRDDImpl(succinctPartitions, succinctSeparators, succinctSchema, minRow, maxRow, succinctSerializer)
      .cache()
  }

  /**
   * Converts an inputRDD to a [[SuccinctTableRDD]], using a list of input separators
   *
   * @param inputRDD The input RDD.
   * @param separators An array of separators for the attributes.
   * @param schema The schema for the RDD.
   * @return The [[SuccinctTableRDD]].
   */
  def apply(inputRDD: RDD[Row], separators: Array[Byte], schema: StructType): SuccinctTableRDD = {
    val minRow: Row = min(inputRDD, schema)
    val maxRow: Row = max(inputRDD, schema)
    val limits = getLimits(maxRow, minRow)
    val succinctSerializer = new SuccinctSerializer(schema, separators, limits)
    val succinctPartitions = inputRDD.mapPartitions {
      partition => createSuccinctBuffer(partition, succinctSerializer)
    }
    new SuccinctTableRDDImpl(succinctPartitions, separators, schema, minRow, maxRow, succinctSerializer)
      .cache()
  }

  /**
   * Converts an inputRDD to a [[SuccinctTableRDD]], using a list of separators that are assumed to be unused
   * in the input data (ASCII 11 onwards).
   *
   * @param inputRDD The input RDD.
   * @param schema The schema for the RDD.
   * @return The [[SuccinctTableRDD]].
   */
  def apply(inputRDD: RDD[Row], schema: StructType): SuccinctTableRDD = {
    // Assume ASCII values 11- are unused in the original text
    val separatorsSize = schema.length
    val separators: Array[Byte] = range(11, 11 + separatorsSize).map(_.toByte)
    val minRow: Row = min(inputRDD, schema)
    val maxRow: Row = max(inputRDD, schema)
    val limits = getLimits(maxRow, minRow)
    val succinctSerializer = new SuccinctSerializer(schema, separators, limits)
    val succinctPartitions = inputRDD.mapPartitions {
      partition => createSuccinctBuffer(partition, succinctSerializer)
    }
    new SuccinctTableRDDImpl(succinctPartitions, separators, schema, minRow, maxRow, succinctSerializer)
      .cache()
  }

  /**
   * Converts a data frame to a [[SuccinctTableRDD]], using a list of separators that are assumed to be unused
   * in the input data (ASCII 11 onwards).
   *
   * @param dataFrame The input data frame.
   * @return The [[SuccinctTableRDD]].
   */
  def apply(dataFrame: DataFrame): SuccinctTableRDD = {
    apply(dataFrame.rdd, dataFrame.schema)
  }

  /**
   * Creates a [[SuccinctIndexedFile]] from an Iterator over [[Row]] and the list of separators.
   *
   * @param dataIter The Iterator over data tuples.
   * @param succinctSerializer The serializer/deserializer for Succinct's representation of records.
   * @return An Iterator over the [[SuccinctIndexedFile]].
   */
  private[succinct] def createSuccinctBuffer(
                                              dataIter: Iterator[Row],
                                              succinctSerializer: SuccinctSerializer): Iterator[SuccinctIndexedFile] = {

    var offsets = new ArrayBuffer[Int]()
    var buffers = new ArrayBuffer[Array[Byte]]()
    var offset = 0
    var partitionSize = 0
    while (dataIter.hasNext) {
      val curTuple = succinctSerializer.serializeRow(dataIter.next())
      buffers += curTuple
      partitionSize += (curTuple.size + 1)
      offsets += offset
      offset += (curTuple.length + 1)
    }

    val rawBufferOS = new ByteArrayOutputStream(partitionSize)
    for (i <- 0 to buffers.size - 1) {
      val curRecord = buffers(i)
      rawBufferOS.write(curRecord)
      rawBufferOS.write(SuccinctCore.EOL)
    }

    val ret = Iterator(new SuccinctIndexedFileBuffer(rawBufferOS.toByteArray, offsets.toArray, 0))
    ret
  }

  private def getLength(data: Any): Int = {
    data match {
      case _: Boolean => 1
      case _: Byte => data.toString.length
      case _: Short => data.toString.length
      case _: Int => data.toString.length
      case _: Long => data.toString.length
      case _: Float => "%.2f".format(data.asInstanceOf[Float]).length
      case _: Double => "%.2f".format(data.asInstanceOf[Double]).length
      case _: java.math.BigDecimal => data.asInstanceOf[java.math.BigDecimal].longValue.toString.length
      case _: String => data.asInstanceOf[String].length
      case _: UTF8String => data.asInstanceOf[UTF8String].length
      case other => throw new IllegalArgumentException(s"Unexpected type.")
    }
  }

  private def getLimits(maximums: Row, minimums: Row): Seq[Int] = {
    val maxLengths = maximums.toSeq.map(getLength)
    val minLengths = minimums.toSeq.map(getLength)
    maxLengths.zip(minLengths).map(x => if (x._1 > x._2) x._1 else x._2)
  }

  private[succinct] def min(inputRDD: RDD[Row], schema: StructType): Row = {
    val absMaxRow = Row.fromSeq(schema.fields.map(f => f.dataType match {
      case BooleanType => true
      case ByteType => Byte.MaxValue
      case ShortType => Short.MaxValue
      case IntegerType => Int.MaxValue
      case LongType => Long.MaxValue
      case FloatType => Float.MaxValue
      case DoubleType => Double.MaxValue
      case StringType => new String(Array[Byte](255.toByte))
      case _: DecimalType => java.math.BigDecimal.valueOf(Double.MaxValue)
      case other => throw new IllegalArgumentException(s"Unexpected type. $other")
    }).toSeq)

    val minRow = inputRDD.fold(absMaxRow)(min)
    minRow
  }

  private[succinct] def max(inputRDD: RDD[Row], schema: StructType): Row = {
    val absMinRow = Row.fromSeq(schema.fields.map(f => f.dataType match {
      case BooleanType => false
      case ByteType => Byte.MinValue
      case ShortType => Short.MinValue
      case IntegerType => Int.MinValue
      case LongType => Long.MinValue
      case FloatType => Float.MinValue
      case DoubleType => Double.MinValue
      case StringType => ""
      case _: DecimalType => java.math.BigDecimal.valueOf(Double.MinValue)
      case other => throw new IllegalArgumentException(s"Unexpected type. $other")
    }).toSeq)

    val maxRow = inputRDD.fold(absMinRow)(max)
    maxRow
  }

  private[succinct] def min(a: Row, b: Row): Row = {
    assert(a.length == b.length)
    val resArr = new Array[Any](a.length)
    for (i <- 0 to a.length - 1) {
      resArr(i) = minValue(a.get(i), b.get(i))
    }
    Row.fromSeq(resArr.toSeq)
  }

  private[succinct] def max(a: Row, b: Row): Row = {
    assert(a.length == b.length)
    val resArr = new Array[Any](a.length)
    for (i <- 0 to a.length - 1) {
      resArr(i) = maxValue(a.get(i), b.get(i))
    }
    Row.fromSeq(resArr.toSeq)
  }

  private[succinct] def minValue(a: Any, b: Any): Any = {
    if (a == null) return b
    if (b == null) return a
    a match {
      case _: Boolean => if (a.asInstanceOf[Boolean] < b.asInstanceOf[Boolean]) a else b
      case _: Byte => if (a.asInstanceOf[Byte] < b.asInstanceOf[Byte]) a else b
      case _: Short => if (a.asInstanceOf[Short] < b.asInstanceOf[Short]) a else b
      case _: Int => if (a.asInstanceOf[Int] < b.asInstanceOf[Int]) a else b
      case _: Long => if (a.asInstanceOf[Long] < b.asInstanceOf[Long]) a else b
      case _: Float => if (a.asInstanceOf[Float] < b.asInstanceOf[Float]) a else b
      case _: Double => if (a.asInstanceOf[Double] < b.asInstanceOf[Double]) a else b
      case _: java.math.BigDecimal =>
        if (a.asInstanceOf[java.math.BigDecimal].compareTo(b.asInstanceOf[java.math.BigDecimal]) < 0) a
        else b
      case _: BigDecimal => if (a.asInstanceOf[BigDecimal] < b.asInstanceOf[BigDecimal]) a else b
      case _: Decimal => {
        b match {
          case _: java.math.BigDecimal => minValue(a.asInstanceOf[Decimal].toJavaBigDecimal, b)
          case _: BigDecimal => minValue(a.asInstanceOf[Decimal].toBigDecimal, b)
          case _: Decimal => minValue(a, b)
          case other => throw new IllegalArgumentException(s"Unexpected type. ${other.getClass}")
        }
      }
      case _: String => if (a.asInstanceOf[String] < b.asInstanceOf[String]) a else b
      case _: UTF8String => {
        b match {
          case _: String => minValue(a.asInstanceOf[UTF8String].toString(), b)
          case _: UTF8String => minValue(a.asInstanceOf[UTF8String].toString(), b.asInstanceOf[UTF8String].toString())
          case other => throw new IllegalArgumentException(s"Unexpected type. ${other.getClass}")
        }
      }
      case other => throw new IllegalArgumentException(s"Unexpected type. ${other.getClass}")
    }
  }

  private[succinct] def maxValue(a: Any, b: Any): Any = {
    if (a == null) return b
    if (b == null) return a
    a match {
      case _: Boolean => if (a.asInstanceOf[Boolean] > b.asInstanceOf[Boolean]) a else b
      case _: Byte => if (a.asInstanceOf[Byte] > b.asInstanceOf[Byte]) a else b
      case _: Short => if (a.asInstanceOf[Short] > b.asInstanceOf[Short]) a else b
      case _: Int => if (a.asInstanceOf[Int] > b.asInstanceOf[Int]) a else b
      case _: Long => if (a.asInstanceOf[Long] > b.asInstanceOf[Long]) a else b
      case _: Float => if (a.asInstanceOf[Float] > b.asInstanceOf[Float]) a else b
      case _: Double => if (a.asInstanceOf[Double] > b.asInstanceOf[Double]) a else b
      case _: java.math.BigDecimal =>
        if (a.asInstanceOf[java.math.BigDecimal].compareTo(b.asInstanceOf[java.math.BigDecimal]) > 0) a
        else b
      case _: BigDecimal => if (a.asInstanceOf[BigDecimal] > b.asInstanceOf[BigDecimal]) a else b
      case _: Decimal => {
        b match {
          case _: java.math.BigDecimal => maxValue(a.asInstanceOf[Decimal].toJavaBigDecimal, b)
          case _: BigDecimal => maxValue(a.asInstanceOf[Decimal].toBigDecimal, b)
          case _: Decimal => maxValue(a, b)
          case other => throw new IllegalArgumentException(s"Unexpected type. ${other.getClass}")
        }
      }
      case _: String => if (a.asInstanceOf[String] > b.asInstanceOf[String]) a else b
      case _: UTF8String => {
        b match {
          case _: String => maxValue(a.asInstanceOf[UTF8String].toString(), b)
          case _: UTF8String => maxValue(a.asInstanceOf[UTF8String].toString(), b.asInstanceOf[UTF8String].toString())
          case other => throw new IllegalArgumentException(s"Unexpected type. ${other.getClass}")
        }
      }
      case other => throw new IllegalArgumentException(s"Unexpected type. ${other.getClass}")
    }
  }

}
