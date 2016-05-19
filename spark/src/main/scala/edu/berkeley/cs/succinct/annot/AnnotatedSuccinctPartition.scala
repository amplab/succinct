package org.apache.spark.succinct.annot

import java.io.{ObjectInputStream, ObjectOutputStream}

import edu.berkeley.cs.succinct.SuccinctIndexedFile
import edu.berkeley.cs.succinct.annot.{Result, Operator}
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.buffers.annot._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.util.{KnownSizeEstimation, SizeEstimator}
import scala.collection.JavaConverters._

import scala.io.Source

class AnnotatedSuccinctPartition(keys: Array[String], documentBuffer: SuccinctIndexedFile,
                                 annotBufferMap: Map[String, SuccinctAnnotationBuffer])
  extends KnownSizeEstimation with Serializable {

  def iterator: Iterator[(String, String)] = {
    new Iterator[(String, String)] {
      var currentRecordId = 0

      override def hasNext: Boolean = currentRecordId < keys.length

      override def next(): (String, String) = {
        val curKey = keys(currentRecordId)
        val curVal = new String(documentBuffer.getRecord(currentRecordId))
        currentRecordId += 1
        (curKey, curVal)
      }
    }
  }

  def save(location: String): Unit = {
    val pathDoc = new Path(location + ".sdocs")
    val pathDocIds = new Path(location + ".sdocids")

    val fs = FileSystem.get(pathDoc.toUri, new Configuration())

    val osDoc = fs.create(pathDoc)
    val osDocIds = new ObjectOutputStream(fs.create(pathDocIds))

    documentBuffer.writeToStream(osDoc)
    osDocIds.writeObject(keys)

    osDoc.close()
    osDocIds.close()

    // Write annotation buffers
    val pathAnnotToc = new Path(location + ".sannots.toc")
    val pathAnnot = new Path(location + ".sannots")
    val osAnnotToc = fs.create(pathAnnotToc)
    val osAnnot = fs.create(pathAnnot)
    annotBufferMap.foreach(kv => {
      val key = kv._1
      val buf = kv._2
      val startPos = osAnnot.getPos
      buf.writeToStream(osAnnot)
      val endPos = osAnnot.getPos
      val size = endPos - startPos
      // Add entry to TOC
      osAnnotToc.writeBytes(s"$key\t$startPos\t$size\n")
    })
    osAnnotToc.close()
    osAnnot.close()
  }

  override def estimatedSize: Long = {
    val docSize = documentBuffer.getCompressedSize
    val annotSize = annotBufferMap.values.map(_.getCompressedSize).sum
    val docIdsSize = SizeEstimator.estimate(keys)
    docSize + annotSize + docIdsSize
  }

  /** Find the index of a particular key using binary search. **/
  def findKey(key: String): Int = {
    var (low, high) = (0, keys.length - 1)

    while (low <= high)
      (low + high) / 2 match {
        case mid if Ordering.String.gt(keys(mid), key) => high = mid - 1
        case mid if Ordering.String.lt(keys(mid), key) => low = mid + 1
        case mid => return mid
      }
    -1
  }

  def getDocument(docId: String): String = {
    val pos = findKey(docId)
    if (pos < 0 || pos > keys.length) null else documentBuffer.getRecord(pos)
  }

  def extractDocument(docId: String, offset: Int, length: Int): String = {
    val pos = findKey(docId)
    if (pos < 0 || pos > keys.length) null else documentBuffer.extractRecord(pos, offset, length)
  }

  def search(query: String): Iterator[(String, Int, Int)] = {
    new Iterator[(String, Int, Int)] {
      val searchIterator = documentBuffer.searchIterator(query.toCharArray)
      val matchLength = query.length

      override def hasNext: Boolean = searchIterator.hasNext

      override def next(): (String, Int, Int) = {
        val offset = searchIterator.next().toInt
        val recordId = documentBuffer.offsetToRecordId(offset)
        val key = keys(recordId)
        val begin = offset - documentBuffer.getRecordOffset(recordId)
        val end = begin + matchLength
        (key, begin, end)
      }
    }
  }

  def regexSearch(query: String): Iterator[(String, Int, Int)] = {
    new Iterator[(String, Int, Int)] {
      val matches = documentBuffer.regexSearch(query).iterator()

      override def hasNext: Boolean = matches.hasNext

      override def next(): (String, Int, Int) = {
        val m = matches.next()
        val offset = m.getOffset.toInt
        val recordId = documentBuffer.offsetToRecordId(offset)
        val key = keys(recordId)
        val begin = offset - documentBuffer.getRecordOffset(recordId)
        val end = begin + m.getLength
        (key, begin, end)
      }
    }
  }

  def filterAnnotations(annotClassFilter: String, annotTypeFilter: String): Iterator[Annotation] = {
    val delim = "\\" + SuccinctAnnotationBuffer.DELIM
    val keyFilter = delim + annotClassFilter + delim + annotTypeFilter + delim
    val iters = annotBufferMap.filterKeys(_ matches keyFilter).values.map(_.iterator().asScala)
    iters.foldLeft(Iterator[Annotation]())(_ ++ _)
  }

  def searchContaining(query: String, annotClass: String, annotType: String): Iterator[Annotation] = {
    val it = search(query)
    val delim = SuccinctAnnotationBuffer.DELIM
    val annotKey = delim + annotClass + delim + annotType + delim
    val buffer = annotBufferMap(annotKey)
    it.flatMap(r => {
      val ar = buffer.getAnnotationRecord(r._1)
      if (ar == null)
        Array[Annotation]()
      else
        ar.findAnnotationsContaining(r._2, r._3).map(ar.getAnnotation)
    })
  }

  def regexContaining(rexp: String, annotClass: String, annotType: String): Iterator[Annotation] = {
    val it = regexSearch(rexp)
    val delim = SuccinctAnnotationBuffer.DELIM
    val annotKey = delim + annotClass + delim + annotType + delim
    val buffer = annotBufferMap(annotKey)
    it.flatMap(r => {
      val ar = buffer.getAnnotationRecord(r._1)
      if (ar == null)
        Array[Annotation]()
      else
        ar.findAnnotationsContaining(r._2, r._3).map(ar.getAnnotation)
    })
  }

  def query(operator: Operator): Iterator[Result] = {
    Iterator[Result]()
  }

  def count: Int = keys.length
}

object AnnotatedSuccinctPartition {
  def apply(partitionLocation: String, annotClassFilter: String, annotTypeFilter: String)
  : AnnotatedSuccinctPartition = {

    val pathDoc = new Path(partitionLocation + ".sdocs")
    val pathDocIds = new Path(partitionLocation + ".sdocids")

    val fs = FileSystem.get(pathDoc.toUri, new Configuration())

    val isDoc = fs.open(pathDoc)
    val isDocIds = new ObjectInputStream(fs.open(pathDocIds))

    val docSize: Int = fs.getContentSummary(pathDoc).getLength.toInt

    val documentBuffer = new SuccinctIndexedFileBuffer(isDoc, docSize)
    val keys = isDocIds.readObject().asInstanceOf[Array[String]]

    isDoc.close()
    isDocIds.close()

    val delim = "\\" + SuccinctAnnotationBuffer.DELIM
    val keyFilter = delim + annotClassFilter + delim + annotTypeFilter + delim
    val pathAnnotToc = new Path(partitionLocation + ".sannots.toc")
    val pathAnnot = new Path(partitionLocation + ".sannots")
    val isAnnotToc = fs.open(pathAnnotToc)
    val annotBufMap = Source.fromInputStream(isAnnotToc).getLines().map(_.split('\t'))
      .map(e => (e(0), e(1).toLong, e(2).toLong))
      .filter(e => e._1 matches keyFilter)
      .map(e => {
        val key = e._1
        val annotClass = key.split('^')(1)
        val annotType = key.split('^')(2)
        val isAnnot = fs.open(pathAnnot)
        isAnnot.seek(e._2)
        val buf = new SuccinctAnnotationBuffer(annotClass, annotType, isAnnot, e._3.toInt)
        isAnnot.close()
        (key, buf)
      }).toMap

    new AnnotatedSuccinctPartition(keys, documentBuffer, annotBufMap)
  }
}
