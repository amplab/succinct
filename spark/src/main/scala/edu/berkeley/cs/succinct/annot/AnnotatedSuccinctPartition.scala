package org.apache.spark.succinct.annot

import java.io.{ObjectInputStream, ObjectOutputStream}

import edu.berkeley.cs.succinct.SuccinctIndexedFile
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.buffers.annot._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.util.{KnownSizeEstimation, SizeEstimator}

class AnnotatedSuccinctPartition(keys: Array[String], documentBuffer: SuccinctIndexedFile,
                                 annotBufferMap: Map[String, AnnotatedSuccinctBuffer])
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
    annotBufferMap.foreach(kv => {
      val suffix = kv._1.replace(AnnotatedSuccinctBuffer.DELIM, '.') + "sannots"
      val pathAnnot = new Path(location + suffix)
      val osAnnot = fs.create(pathAnnot)
      kv._2.writeToStream(osAnnot)
      osAnnot.close()
    })
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
    val delim = AnnotatedSuccinctBuffer.DELIM
    val annotKey = delim + annotClass + delim + annotType + delim
    val buffer = annotBufferMap(annotKey)
    it.flatMap(r => {
      val ar = buffer.getAnnotationRecord(r._1)
      if (ar == null)
        Array[Annotation]()
      else
        ar.findAnnotationsOver(r._2, r._3).map(ar.getAnnotation)
    })
  }

  def regexOver(rexp: String, annotClass: String, annotType: String): Iterator[Annotation] = {
    val it = regexSearch(rexp)
    val delim = AnnotatedSuccinctBuffer.DELIM
    val annotKey = delim + annotClass + delim + annotType + delim
    val buffer = annotBufferMap(annotKey)
    it.flatMap(r => {
      val ar = buffer.getAnnotationRecord(r._1)
      if (ar == null)
        Array[Annotation]()
      else
        ar.findAnnotationsOver(r._2, r._3).map(ar.getAnnotation)
    })
  }

  def count: Int = keys.length
}

object AnnotatedSuccinctPartition {
  def apply(partitionLocation: String)
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

    def getAnnotKey(filename: String): String = {
      val start = filename.indexOf('.')
      val end = filename.lastIndexOf('.') + 1
      filename.substring(start, end).replace('.', AnnotatedSuccinctBuffer.DELIM)
    }

    val statuses = fs.globStatus(new Path(partitionLocation + ".*.sannots"))
    val annotBufMap = statuses.map(status => {
      val pathAnnot = status.getPath
      val isAnnot = fs.open(pathAnnot)
      val annotSize: Int = status.getLen.toInt
      val annotBuffer = new AnnotatedSuccinctBuffer(isAnnot, annotSize)
      isAnnot.close()
      (getAnnotKey(pathAnnot.getName), annotBuffer)
    }).toMap

    new AnnotatedSuccinctPartition(keys, documentBuffer, annotBufMap)
  }
}
