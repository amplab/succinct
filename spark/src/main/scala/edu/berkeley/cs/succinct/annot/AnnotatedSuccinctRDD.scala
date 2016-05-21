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

  /**
    * Get document text given the documentID.
    *
    * @param documentId The documentID for the document.
    * @return The document text.
    */
  def getDocument(documentId: String): String = {
    val values = partitionsRDD.map(buf => buf.getDocument(documentId)).filter(v => v != null).collect()
    if (values.length > 1) {
      throw new IllegalStateException(s"DocumentId ${documentId.toString} returned ${values.length} values")
    }
    if (values.length == 0) null else values(0)
  }

  /**
    * Get document text at a specified offset given the documentID.
    *
    * @param documentId The documentID for the document.
    * @param offset     The offset into the document text.
    * @param length     The number of characters to fetch.
    * @return The document text.
    */
  def extractDocument(documentId: String, offset: Int, length: Int): String = {
    val values = partitionsRDD.map(buf => buf.extractDocument(documentId, offset, length)).filter(v => v != null).collect()
    if (values.length > 1) {
      throw new IllegalStateException(s"DocumentId ${documentId.toString} returned ${values.length} values")
    }
    if (values.length == 0) null else values(0)
  }

  /**
    * Find all (documentID, startOffset, endOffset) triplets corresponding to an arbitrary query
    * composed of Contains, ContainedIn, Before, After, FilterAnnotations, Search and RegexSearch
    * queries.
    *
    * @param operator An arbitrary expression tree composed of Contains, ContainedIn, Before, After,
    *                 FilterAnnotations, Search and RegexSearch.
    * @return An [[Iterator]] over matching (documentID, startOffset, endOffset) triplets
    *         encapsulated as Result objects.
    */
  def query(operator: Operator): RDD[Result] = {
    partitionsRDD.flatMap(_.query(operator))
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
  /**
    * Creates an [[AnnotatedSuccinctRDD]] from an RDD of triplets (documentID, documentText, annotations).
    *
    * @param inputRDD          RDD of (documentID, documentText, annotations) triplets.
    * @param ignoreParseErrors Ignores errors in parsing annotations if set to true; throws an exception on error otherwise.
    * @return The [[AnnotatedSuccinctRDD]].
    */
  def apply(inputRDD: RDD[(String, String, String)], ignoreParseErrors: Boolean = true): AnnotatedSuccinctRDD = {
    val partitionsRDD = inputRDD.sortBy(_._1)
      .mapPartitionsWithIndex((idx, it) => createAnnotatedSuccinctPartition(it, ignoreParseErrors)).cache()
    new AnnotatedSuccinctRDDImpl(partitionsRDD)
  }

  /**
    * Reads a AnnotatedSuccinctRDD from disk.
    *
    * @param sc       The spark context
    * @param location The path to read the [[AnnotatedSuccinctRDD]] from.
    * @return The [[AnnotatedSuccinctRDD]].
    */
  def apply(sc: SparkContext, location: String): AnnotatedSuccinctRDD = {
    apply(sc, location, ".*", ".*")
  }

  /**
    * Reads a [[AnnotatedSuccinctRDD]] from disk, based on filters on Annotation Class and Type.
    *
    * @param sc               The spark context
    * @param location         The path to read the SuccinctKVRDD from.
    * @param annotClassFilter Regex filter specifying which annotation classes to load.
    * @param annotTypeFilter  Regex filter specifying which annotation types to load.
    * @return The [[AnnotatedSuccinctRDD]].
    */
  def apply(sc: SparkContext, location: String, annotClassFilter: String, annotTypeFilter: String): AnnotatedSuccinctRDD = {
    val locationPath = new Path(location)
    val fs = FileSystem.get(locationPath.toUri, sc.hadoopConfiguration)
    val status = fs.listStatus(locationPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.startsWith("part-") && path.getName.endsWith(".sdocs")
      }
    })
    val numPartitions = status.length
    val partitionsRDD = sc.parallelize(0 until numPartitions, numPartitions)
      .mapPartitionsWithIndex[AnnotatedSuccinctPartition]((i, partition) => {
      val partitionLocation = location.stripSuffix("/") + "/part-" + "%05d".format(i)
      Iterator(AnnotatedSuccinctPartition(partitionLocation, annotClassFilter, annotTypeFilter))
    }).cache()
    new AnnotatedSuccinctRDDImpl(partitionsRDD)
  }

  /**
    * Creates an [[AnnotatedSuccinctPartition]] from a collection of (documentID, documentText, annotations) triplets.
    *
    * @param dataIter          An iterator over (documentID, documentText, annotations) triplets.
    * @param ignoreParseErrors Ignores errors in parsing annotations if set to true; throws an exception on error otherwise.
    * @return An iterator over [[AnnotatedSuccinctPartition]]
    */
  def createAnnotatedSuccinctPartition(dataIter: Iterator[(String, String, String)], ignoreParseErrors: Boolean):
  Iterator[AnnotatedSuccinctPartition] = {
    val serializer = new AnnotatedDocumentSerializer(ignoreParseErrors)
    serializer.serialize(dataIter)

    val docIds = serializer.getDocIds
    val docTextBuffer = serializer.getTextBuffer
    val succinctDocTextBuffer = new SuccinctIndexedFileBuffer(docTextBuffer._2, docTextBuffer._1)
    val succinctAnnotBufferMap = serializer.getAnnotationBuffers.map(kv => {
      val key = kv._1
      val annotClass = key.split('^')(1)
      val annotType = key.split('^')(2)
      (key, new SuccinctAnnotationBuffer(annotClass, annotType, kv._2))
    })
    Iterator(new AnnotatedSuccinctPartition(docIds, succinctDocTextBuffer, succinctAnnotBufferMap))
  }


}
