package edu.berkeley.cs.succinct.json

import collection.JavaConversions._

import edu.berkeley.cs.succinct.block.json.{FieldMapping, JsonBlockSerializer}
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.json.impl.SuccinctJsonRDDImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

/**
  * A compressed RDD containing a collection of JSON documents, represented using Succinct's data
  * structures. The RDD supports get, search and filter operations. Each document is internally
  * assigned a unique "id" field, which is used for get, search and filter operations. The search
  * and filter operations return an RDD of these ids, and the get operation obtains the uncompressed
  * version of the JSON document for a given id.
  *
  */
abstract class SuccinctJsonRDD(@transient sc: SparkContext,
    @transient deps: Seq[Dependency[_]])
  extends RDD[String](sc, deps) {

  /**
    * Returns the RDD of partitions.
    *
    * @return The RDD of partitions.
    */
  private[succinct] def partitionsRDD: RDD[SuccinctJsonPartition]

  /**
    * Returns first parent of the RDD.
    *
    * @return The first parent of the RDD.
    */
  protected[succinct] def getFirstParent: RDD[SuccinctJsonPartition] = {
    firstParent[SuccinctJsonPartition]
  }

  /**
    * Returns the array of partitions.
    *
    * @return The array of partitions.
    */
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
    * Overrides the compute function to iterate over Succinct's representation of JSON documents,
    * and deserialize them into uncompressed JSON documents.
    */
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val succinctIterator = firstParent[SuccinctJsonPartition].iterator(split, context)
    if (succinctIterator.hasNext) {
      succinctIterator.next().jIterator
    } else {
      Iterator[String]()
    }
  }

  /**
    * Count the number of JSON documents in the SuccinctJsonRDD.
    *
    * @return The number of JSON documents in the SuccinctJsonRDD.
    */
  override def count(): Long = {
    partitionsRDD.map(_.count).aggregate(0L)(_ + _, _ + _)
  }

  /**
    * Get the JSON document with the specified ID.
    * @param id The id of the document to fetch.
    * @return The JSON document.
    */
  def get(id: Long): String = {
    val values = partitionsRDD.map(buf => buf.jGet(id)).filter(v => v != null).collect()
    if (values.length > 1) {
      throw new IllegalStateException(s"ID ${id.toString} returned ${values.length} values")
    }
    if (values.length == 0) null else values(0)
  }

  /**
    * Filter the set of JSON documents based on the provided value for a given field.
    *
    * @param field The field to be matched. For nested documents, use dot notation to denote nested
    *              attributes.
    * @param value The value of the field to be matched. The filter operation performs an exact
    *              match.
    * @return An RDD containing the ids for the filtered documents.
    */
  def filter(field: String, value: String): RDD[Long] = {
    partitionsRDD.flatMap(_.jSearch(field, value))
  }

  /**
    * Search for all the documents that contain a particular query string.
    *
    * @param query The query string to be searched for.
    * @return An RDD containing the ids for the filtered documents.
    */
  def search(query: String): RDD[Long] = {
    partitionsRDD.flatMap(_.jSearch(query))
  }
}

/** Factory object for creating [[SuccinctJsonRDD]]. **/
object SuccinctJsonRDD {
  def apply(inputRDD: RDD[String]): SuccinctJsonRDD = {
    val idOffsets = inputRDD.mapPartitions(it => Iterator(it.length)).collect().scanLeft(0L)(_ + _)
    val partitionsRDD = inputRDD.mapPartitionsWithIndex((idx, it) =>
      createSuccinctJsonPartition(it, idOffsets(idx), idOffsets(idx + 1) - 1))
    new SuccinctJsonRDDImpl(partitionsRDD)
  }

  def createSuccinctJsonPartition(dataIter: Iterator[String], idBegin: Long, idEnd: Long):
    Iterator[SuccinctJsonPartition] = {
    val serializer = new JsonBlockSerializer((-120 to -1).toArray.map(_.toByte))
    val serializedData = serializer.serialize(dataIter)
    val valueBuffer = new SuccinctIndexedFileBuffer(serializedData.getData,
      serializedData.getOffsets)
    val fieldMapping = serializedData.getMetadata.asInstanceOf[FieldMapping]
    Iterator(new SuccinctJsonPartition((idBegin to idEnd).toArray, valueBuffer, fieldMapping))
  }
}
