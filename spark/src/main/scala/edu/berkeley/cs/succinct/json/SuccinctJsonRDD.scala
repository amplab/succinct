package edu.berkeley.cs.succinct.json

import edu.berkeley.cs.succinct.block.json.{FieldMapping, JsonBlockSerializer}
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.json.impl.SuccinctJsonRDDImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._

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

  def get(id: Long): String = {
    val values = partitionsRDD.map(buf => buf.jGet(id)).filter(v => v != null).collect()
    if (values.length > 1) {
      throw new IllegalStateException(s"ID ${id.toString} returned ${values.length} values")
    }
    if (values.length == 0) null else values(0)
  }

  def search(field: String, value: String): RDD[Long] = {
    partitionsRDD.flatMap(_.jSearch(field, value))
  }
}

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
