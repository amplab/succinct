package org.apache.spark.succinct.annot

import java.io.{DataOutputStream, ObjectInputStream, ObjectOutputStream}

import edu.berkeley.cs.succinct.SuccinctIndexedFile
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.buffers.annot._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.util.{KnownSizeEstimation, SizeEstimator}

class AnnotatedSuccinctPartition(keys: Array[String], documentBuffer: SuccinctIndexedFile,
                                 annotationBuffer: AnnotatedSuccinctBuffer)
  extends KnownSizeEstimation {

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

  def writeToStream(dataStream: DataOutputStream) = {
    documentBuffer.writeToStream(dataStream)
    annotationBuffer.writeToStream(dataStream)
    val objectOutputStream = new ObjectOutputStream(dataStream)
    objectOutputStream.writeObject(keys)
  }

  override def estimatedSize: Long = {
    documentBuffer.getSuccinctIndexedFileSize + annotationBuffer.getSuccinctFileSize
    +SizeEstimator.estimate(keys)
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
    if (pos < 0 || pos > keys.length) null else new String(documentBuffer.getRecord(pos))
  }

  def search(query: String): Iterator[(String, Int, Int)] = {
    new Iterator[(String, Int, Int)] {
      val searchIterator = documentBuffer.searchIterator(query.getBytes())
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
    it.flatMap(r => {
      val ar = annotationBuffer.findAnnotationRecord(r._1, annotClass, annotType)
      annotationBuffer.findAnnotationsOver(ar, r._2, r._3)
        .map(i => annotationBuffer.getAnnotation(ar, i))
    })
  }

  def regexOver(rexp: String, annotClass: String, annotType: String): Iterator[Annotation] = {
    val it = regexSearch(rexp)
    it.flatMap(r => {
      val ar = annotationBuffer.findAnnotationRecord(r._1, annotClass, annotType)
      annotationBuffer.findAnnotationsOver(ar, r._2, r._3)
        .map(i => annotationBuffer.getAnnotation(ar, i))
    })
  }

  def count: Int = keys.length
}

object AnnotatedSuccinctPartition {
  def apply(partitionLocation: String)
  : AnnotatedSuccinctPartition = {

    val path = new Path(partitionLocation)
    val fs = FileSystem.get(path.toUri, new Configuration())
    val is = fs.open(path)
    val documentBuffer = new SuccinctIndexedFileBuffer(is)
    val annotationBuffer = new AnnotatedSuccinctBuffer(is)

    val ois = new ObjectInputStream(is)
    val keys = ois.readObject().asInstanceOf[Array[String]]
    is.close()

    new AnnotatedSuccinctPartition(keys, documentBuffer, annotationBuffer)
  }
}
