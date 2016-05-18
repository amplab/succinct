package edu.berkeley.cs.succinct.annot

import edu.berkeley.cs.succinct.annot.impl.AnnotatedSuccinctRDDImpl
import edu.berkeley.cs.succinct.block.AnnotatedDocumentSerializer
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.buffers.annot._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.succinct.annot.AnnotatedSuccinctPartition
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

abstract class AnnotatedSuccinctRDD(@transient sc: SparkContext,
                                    @transient deps: Seq[Dependency[_]])
  extends RDD[(String, String)](sc, deps) {

  /**
    * Returns the RDD of partitions.
    *
    * @return The RDD of partitions.
    */
  private[succinct] def partitionsRDD: RDD[AnnotatedSuccinctPartition]

  /**
    * Returns first parent of the RDD.
    *
    * @return The first parent of the RDD.
    */
  protected[succinct] def getFirstParent: RDD[AnnotatedSuccinctPartition] = {
    firstParent[AnnotatedSuccinctPartition]
  }

  /**
    * Returns the array of partitions.
    *
    * @return The array of partitions.
    */
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /** Overrides the compute function to return iterator over Succinct records. */
  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val succinctIterator = firstParent[AnnotatedSuccinctPartition].iterator(split, context)
    if (succinctIterator.hasNext) {
      succinctIterator.next().iterator
    } else {
      Iterator[(String, String)]()
    }
  }

  def getDocument(documentId: String): String = {
    val values = partitionsRDD.map(buf => buf.getDocument(documentId)).filter(v => v != null).collect()
    if (values.length > 1) {
      throw new IllegalStateException(s"DocumentId ${documentId.toString} returned ${values.length} values")
    }
    if (values.length == 0) null else values(0)
  }

  def extractDocument(documentId: String, offset: Int, length: Int): String = {
    val values = partitionsRDD.map(buf => buf.extractDocument(documentId, offset, length)).filter(v => v != null).collect()
    if (values.length > 1) {
      throw new IllegalStateException(s"DocumentId ${documentId.toString} returned ${values.length} values")
    }
    if (values.length == 0) null else values(0)
  }

  def search(query: String): RDD[(String, Int, Int)] = {
    partitionsRDD.flatMap(_.search(query))
  }

  def regexSearch(query: String): RDD[(String, Int, Int)] = {
    partitionsRDD.flatMap(_.regexSearch(query))
  }

  def searchOver(query: String, annotClass: String, annotType: String): RDD[Annotation] = {
    partitionsRDD.flatMap(_.searchOver(query, annotClass, annotType))
  }

  def regexOver(query: String, annotClass: String, annotType: String): RDD[Annotation] = {
    partitionsRDD.flatMap(_.regexOver(query, annotClass, annotType))
  }

  /**
    * Saves the [[AnnotatedSuccinctRDD]] at the specified path.
    *
    * @param location The path where the [[AnnotatedSuccinctRDD]] should be stored.
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

      partition.save(partitionLocation)
    })

    val successPath = new Path(location.stripSuffix("/") + "/_SUCCESS")
    fs.create(successPath).close()
  }

}

object AnnotatedSuccinctRDD {
  def apply(inputRDD: RDD[(String, String, String)]): AnnotatedSuccinctRDD = {
    val partitionsRDD = inputRDD.sortBy(_._1)
      .mapPartitionsWithIndex((idx, it) => createAnnotatedSuccinctPartition(it))
    new AnnotatedSuccinctRDDImpl(partitionsRDD)
  }

  /**
    * Reads a AnnotatedSuccinctRDD from disk.
    *
    * @param sc       The spark context
    * @param location The path to read the SuccinctKVRDD from.
    * @return The SuccinctKVRDD.
    */
  def apply(sc: SparkContext, location: String, annotClassFilter: String = ".*", annotTypeFilter: String = ".*"): AnnotatedSuccinctRDD = {
    val locationPath = new Path(location)
    val fs = FileSystem.get(locationPath.toUri, sc.hadoopConfiguration)
    val status = fs.listStatus(locationPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.startsWith("part-") && path.getName.endsWith(".sdocs")
      }
    })
    val numPartitions = status.length
    val succinctPartitions = sc.parallelize(0 until numPartitions, numPartitions)
      .mapPartitionsWithIndex[AnnotatedSuccinctPartition]((i, partition) => {
      val partitionLocation = location.stripSuffix("/") + "/part-" + "%05d".format(i)
      Iterator(AnnotatedSuccinctPartition(partitionLocation, annotClassFilter, annotTypeFilter))
    }).cache()
    new AnnotatedSuccinctRDDImpl(succinctPartitions)
  }

  def createAnnotatedSuccinctPartition(dataIter: Iterator[(String, String, String)]):
  Iterator[AnnotatedSuccinctPartition] = {
    val serializer = new AnnotatedDocumentSerializer
    serializer.serialize(dataIter)

    val docIds = serializer.getDocIds
    val docTextBuffer = serializer.getTextBuffer
    val succinctDocTextBuffer = new SuccinctIndexedFileBuffer(docTextBuffer._2, docTextBuffer._1)
    val succinctAnnotBufferMap = serializer.getAnnotationBuffers.mapValues(v => new SuccinctAnnotationBuffer(v))
    Iterator(new AnnotatedSuccinctPartition(docIds, succinctDocTextBuffer, succinctAnnotBufferMap))
  }


}
