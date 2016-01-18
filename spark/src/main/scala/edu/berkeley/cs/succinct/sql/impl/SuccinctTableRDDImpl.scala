package edu.berkeley.cs.succinct.sql.impl

import edu.berkeley.cs.succinct.SuccinctIndexedFile.QueryType
import edu.berkeley.cs.succinct.sql._
import edu.berkeley.cs.succinct.{SuccinctCore, SuccinctIndexedFile}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{Decimal, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.succinct.sql.SuccinctTablePartition
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

/**
 * Implements [[SuccinctTableRDD]]; provides implementations for the count and search methods.
 *
 * @constructor Creates a [[SuccinctTableRDD]] from an RDD of [[SuccinctIndexedFile]] partitions,
 *              the list of separators and the target storage level.
 * @param partitionsRDD The RDD of partitions (SuccinctTablePartition).
 * @param separators The list of separators for distinguishing between attributes.
 * @param schema The schema for [[SuccinctTableRDD]]
 * @param targetStorageLevel The target storage level for the RDD.
 */
class SuccinctTableRDDImpl private[succinct](
    val partitionsRDD: RDD[SuccinctTablePartition],
    val separators: Array[Byte],
    val schema: StructType,
    val minimums: Row,
    val maximums: Row,
    val succinctSerializer: SuccinctSerDe,
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends SuccinctTableRDD(partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  val recordCount = partitionsRDD.map(_.count).aggregate(0L)(_ + _,  _ + _)

  /** Overrides compute to return an iterator over Succinct's representation of rows. */
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val succinctIterator = firstParent[SuccinctTablePartition].iterator(split, context)
    if (succinctIterator.hasNext) {
      succinctIterator.next().iterator
    } else {
      Iterator[Row]()
    }
  }

  /** Set the name for the RDD; By default set to "SuccinctTableRDD". */
  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }

  setName("SuccinctTableRDD")

  /**
   * Persists the Succinct partitions at the specified storage level, ignoring any existing target
   * storage level.
   */
  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  /** Un-persists the Succinct partitions using the specified blocking mode. */
  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  /** Persists the Succinct partitions at `targetStorageLevel`, which defaults to MEMORY_ONLY. */
  override def cache(): this.type = {
    partitionsRDD.persist(targetStorageLevel)
    this
  }

  /** Implements createQuery for [[SuccinctTableRDD]] */
  private[sql] def createQuery(attrIdx: Int, query: Array[Byte]): Array[Byte] = {
    getSeparator(attrIdx) +: query :+ getSeparator(attrIdx + 1)
  }

  /** Implements createQuery for [[SuccinctTableRDD]] */
  private[sql] def createQuery(attribute: String, query: Array[Byte]): Array[Byte] = {
    val attrIdx = getAttrIdx(attribute)
    createQuery(attrIdx, query)
  }

  /** Implements getAttrIdx for [[SuccinctTableRDD]] */
  private[sql] def getAttrIdx(attribute: String): Int = schema.lastIndexOf(schema(attribute))

  /** Implements getSeparator for [[SuccinctTableRDD]] */
  private[sql] def getSeparator(attrIdx: Int): Byte = {
    if (attrIdx == separators.length) SuccinctCore.EOL
    else separators(attrIdx)
  }

  /**
   * Converts filters to queries for SuccinctIndexedBuffer's recordMultiSearchIds.
   *
   * @param filters Array of filters to be applied.
   * @return Array of queries.
   */
  private[sql] def filtersToQueries(filters: Array[Filter]): Array[(QueryType, Array[Array[Byte]])] = {
    filters.filter(isFilterSupported).map {
      case StringStartsWith(attribute, value) =>
        (QueryType.Search, Array[Array[Byte]](createPrefixQuery(attribute, value.getBytes)))

      case StringEndsWith(attribute, value) =>
        (QueryType.Search, Array[Array[Byte]](createSuffixQuery(attribute, value.getBytes)))

      case StringContains(attribute, value) =>
        (QueryType.Search, Array[Array[Byte]](value.getBytes))

      case EqualTo(attribute, value) =>
        val attrIdx = getAttrIdx(attribute)
        val query = succinctSerializer.typeToString(attrIdx, value).getBytes
        (QueryType.Search, Array[Array[Byte]](createQuery(attribute, query)))

      case LessThanOrEqual(attribute, value) =>
        val attrIdx = getAttrIdx(attribute)
        val mValue = SuccinctTableRDD.minValue(value, maximums.get(attrIdx))
        val minValue = succinctSerializer.typeToString(attrIdx, minimums.get(attrIdx)).getBytes
        val maxValue = succinctSerializer.typeToString(attrIdx, mValue).getBytes
        val queryBegin = createQuery(attrIdx, minValue)
        val queryEnd = createQuery(attrIdx, maxValue)
        (QueryType.RangeSearch, Array[Array[Byte]](queryBegin, queryEnd))

      case GreaterThanOrEqual(attribute, value) =>
        val attrIdx = getAttrIdx(attribute)
        val mValue = SuccinctTableRDD.maxValue(value, minimums.get(attrIdx))
        val minValue = succinctSerializer.typeToString(attrIdx, mValue).getBytes
        val maxValue = succinctSerializer.typeToString(attrIdx, maximums.get(attrIdx)).getBytes
        val queryBegin = createQuery(attrIdx, minValue)
        val queryEnd = createQuery(attrIdx, maxValue)
        (QueryType.RangeSearch, Array[Array[Byte]](queryBegin, queryEnd))

      case LessThan(attribute, value) =>
        val attrIdx = getAttrIdx(attribute)
        val mValue = SuccinctTableRDD.minValue(value, maximums.get(attrIdx))
        val minValue = succinctSerializer.typeToString(attrIdx, minimums.get(attrIdx)).getBytes
        val maxValue = succinctSerializer.typeToString(attrIdx, prevValue(mValue)).getBytes
        val queryBegin = createQuery(attrIdx, minValue)
        val queryEnd = createQuery(attrIdx, maxValue)
        (QueryType.RangeSearch, Array[Array[Byte]](queryBegin, queryEnd))

      case GreaterThan(attribute, value) =>
        val attrIdx = getAttrIdx(attribute)
        val mValue = SuccinctTableRDD.maxValue(value, minimums.get(attrIdx))
        val minValue = succinctSerializer.typeToString(attrIdx, nextValue(mValue)).getBytes
        val maxValue = succinctSerializer.typeToString(attrIdx, maximums.get(attrIdx)).getBytes
        val queryBegin = createQuery(attrIdx, minValue)
        val queryEnd = createQuery(attrIdx, maxValue)
        (QueryType.RangeSearch, Array[Array[Byte]](queryBegin, queryEnd))
    }
  }

  /** Implements createPrefixQuery for [[SuccinctTableRDD]] */
  private[sql] def createPrefixQuery(attribute: String, query: Array[Byte]): Array[Byte] = {
    val attrIdx = schema.lastIndexOf(schema(attribute))
    getSeparator(attrIdx) +: query
  }

  /** Implements createSuffixQuery for [[SuccinctTableRDD]] */
  private[sql] def createSuffixQuery(attribute: String, query: Array[Byte]): Array[Byte] = {
    val attrIdx = schema.lastIndexOf(schema(attribute))
    query :+ getSeparator(attrIdx + 1)
  }

  /**
   * Check if a filter is supported directly by Succinct data structures.
   *
   * @param f Filter to check.
   * @return Returns true if the filter is supported;
   *         false otherwise.
   */
  private[sql] def isFilterSupported(f: Filter): Boolean = f match {
    case StringStartsWith(attribute, value) => true
    case StringEndsWith(attribute, value) => true
    case StringContains(attribute, value) => true
    case EqualTo(attribute, value) => true
    case LessThan(attribute, value) => true
    case LessThanOrEqual(attribute, value) => true
    case GreaterThan(attribute, value) => true
    case GreaterThanOrEqual(attribute, value) => true

    /** Not supported: In, IsNull, IsNotNull, And, Or, Not */
    case _ => false
  }

  /**
   * Gives the previous value for an input value.
   *
   * @param data The input value.
   * @return The previous value.
   */
  private[sql] def prevValue(data: Any): Any = {
    data match {
      case _: Boolean => !data.asInstanceOf[Boolean]
      case _: Byte => data.asInstanceOf[Byte] - 1
      case _: Short => data.asInstanceOf[Short] - 1
      case _: Int => data.asInstanceOf[Int] - 1
      case _: Long => data.asInstanceOf[Long] - 1
      case _: Float => data.asInstanceOf[Float] - Float.MinPositiveValue
      case _: Double => data.asInstanceOf[Double] - Double.MinPositiveValue
      case _: java.math.BigDecimal => data.asInstanceOf[java.math.BigDecimal]
      case _: BigDecimal => data.asInstanceOf[BigDecimal]
      case _: Decimal => data.asInstanceOf[Decimal]
      case _: String => data.asInstanceOf[String]
      case other => throw new IllegalArgumentException(s"Unexpected type.")
    }
  }

  /**
   * Gives the next value for an input value.
   *
   * @param data The input value.
   * @return The next value.
   */
  private[sql] def nextValue(data: Any): Any = {
    data match {
      case _: Boolean => !data.asInstanceOf[Boolean]
      case _: Byte => data.asInstanceOf[Byte] + 1
      case _: Short => data.asInstanceOf[Short] + 1
      case _: Int => data.asInstanceOf[Int] + 1
      case _: Long => data.asInstanceOf[Long] + 1
      case _: Float => data.asInstanceOf[Float] + Float.MinPositiveValue
      case _: Double => data.asInstanceOf[Double] + Double.MinPositiveValue
      case _: java.math.BigDecimal => data.asInstanceOf[java.math.BigDecimal]
      case _: Decimal => data.asInstanceOf[Decimal]
      case _: String => data.asInstanceOf[String]
      case other => throw new IllegalArgumentException(s"Unexpected type.")
    }
  }

  /** Implements pruneAndFilter for [[SuccinctTableRDD]]. */
  override def pruneAndFilter(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val reqColsCheck = schema.map(f => f.name -> requiredColumns.contains(f.name)).toMap
    val queryList = filtersToQueries(filters)
    val queryTypes = queryList.map(_._1)
    val queries = queryList.map(_._2)
    if (queries.length == 0)
      if (requiredColumns.length == schema.length)
        this
      else
        partitionsRDD.flatMap(_.prune(reqColsCheck))
    else
      partitionsRDD.flatMap(_.pruneAndFilter(reqColsCheck, queryTypes, queries))
  }

  /** Implements save for [[SuccinctTableRDD]] */
  override def save(path: String): Unit = {
    val dataPath = path.stripSuffix("/") + "/data"
    val schemaPath = path.stripSuffix("/") + "/schema"
    val separatorsPath = path.stripSuffix("/") + "/separators"
    val minPath = path.stripSuffix("/") + "/min"
    val maxPath = path.stripSuffix("/") + "/max"
    val conf = new Configuration()
    val fs = FileSystem.get(new Path(path.stripSuffix("/")).toUri, conf)
    fs.mkdirs(new Path(dataPath))
    SuccinctUtils.writeObjectToFS(conf, schemaPath, schema)
    SuccinctUtils.writeObjectToFS(conf, separatorsPath, separators)
    SuccinctUtils.writeObjectToFS(conf, minPath, minimums)
    SuccinctUtils.writeObjectToFS(conf, maxPath, maximums)
    partitionsRDD.zipWithIndex().foreach(entry => {
      val i = entry._2
      val partition = entry._1
      val partitionLocation = dataPath + "/part-" + "%05d".format(i)
      val path = new Path(partitionLocation)
      val fs = FileSystem.get(path.toUri, new Configuration())
      val os = fs.create(path)
      partition.writeToStream(os)
      os.close()
    })
    fs.create(new Path(s"${path.stripSuffix("/")}/_SUCCESS")).close()
  }

  /**
   * Get the count of the number of records in the RDD.
   *
   * @return The count of the number of records in the RDD.
   */
  override def count(): Long = {
    recordCount
  }
}
