package org.apache.spark.succinct.annot

import java.io.{ObjectInputStream, ObjectOutputStream}

import edu.berkeley.cs.succinct.SuccinctIndexedFile
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.buffers.annot._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.util.{KnownSizeEstimation, SizeEstimator}

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
    val osAnnotToc = fs.create(pathAnnotToc)
    annotBufferMap.zipWithIndex.foreach(kv => {
      val key = kv._1._1
      val buf = kv._1._2
      val idx = kv._2

      // Add entry to TOC
      osAnnotToc.writeBytes(s"$key\t$idx\n")
      val pathAnnot = new Path(location + ".sannots." + idx)
      val osAnnot = fs.create(pathAnnot)
      buf.writeToStream(osAnnot)
      osAnnot.close()
    })
    osAnnotToc.close()
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

  def searchOver(query: String, annotClass: String, annotType: String): Iterator[Annotation] = {
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

  def regexOver(rexp: String, annotClass: String, annotType: String): Iterator[Annotation] = {
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
    val isAnnotToc = fs.open(pathAnnotToc)
    val annotBufMap = Source.fromInputStream(isAnnotToc).getLines().map(_.split('\t'))
      .map(e => (e(0), new Path(partitionLocation + ".sannots." + e(1))))
      .filter(kv => kv._1 matches keyFilter).toMap
      .mapValues(p => {
        val isAnnot = fs.open(p)
        val annotSize: Int = fs.getContentSummary(p).getLength.toInt
        val buf = new SuccinctAnnotationBuffer(isAnnot, annotSize)
        isAnnot.close()
        buf
      })

    new AnnotatedSuccinctPartition(keys, documentBuffer, annotBufMap)
  }
}
