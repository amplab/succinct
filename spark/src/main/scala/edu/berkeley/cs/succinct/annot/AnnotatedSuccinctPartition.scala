package org.apache.spark.succinct.annot

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util.NoSuchElementException

import edu.berkeley.cs.succinct.SuccinctIndexedFile
import edu.berkeley.cs.succinct.annot._
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

  /**
    * Get an [[Iterator]] over the documents.
    *
    * @return An [[Iterator]] over the documents.
    */
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

  /**
    * Saves the partition at the specified location prefix.
    *
    * @param location The prefix for the partition's save location.
    */
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

  /**
    * Get an estimate of the partition size.
    *
    * @return An estimate of the estimated partition size.
    */
  override def estimatedSize: Long = {
    val docSize = documentBuffer.getCompressedSize
    val annotSize = annotBufferMap.values.map(_.getCompressedSize).sum
    val docIdsSize = SizeEstimator.estimate(keys)
    docSize + annotSize + docIdsSize
  }

  /**
    * Find the index of a particular key using binary search.
    *
    * @param key The key to find.
    * @return Position of the key in the list of keys.
    */
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

  /**
    * Get the document text for a given document ID.
    *
    * @param docId The document ID.
    * @return The document text.
    */
  def getDocument(docId: String): String = {
    val pos = findKey(docId)
    if (pos < 0 || pos > keys.length) null else documentBuffer.getRecord(pos)
  }

  /**
    * Extract the document text for a given document ID at a specified offset into the document.
    *
    * @param docId  The document ID.
    * @param offset The offset into the document.
    * @param length The number of characters to extract.
    * @return The extracted document text.
    */
  def extractDocument(docId: String, offset: Int, length: Int): String = {
    val pos = findKey(docId)
    if (pos < 0 || pos > keys.length) null else documentBuffer.extractRecord(pos, offset, length)
  }

  /**
    * Search for a query string in the document texts.
    *
    * @param query The query string to search for.
    * @return The location and length of the matches for each document.
    */
  def search(query: String): Iterator[Result] = {
    new Iterator[Result] {
      val searchIterator = documentBuffer.searchIterator(query.toCharArray)
      val matchLength = query.length

      override def hasNext: Boolean = searchIterator.hasNext

      override def next(): Result = {
        val offset = searchIterator.next().toInt
        val recordId = documentBuffer.offsetToRecordId(offset)
        val key = keys(recordId)
        val begin = offset - documentBuffer.getRecordOffset(recordId)
        val end = begin + matchLength
        Result(key, begin, end, null)
      }
    }
  }

  /**
    * Search for a regex pattern in the document texts.
    *
    * @param query The regex pattern to search for.
    * @return The location and length of the matches for each document.
    */
  def regexSearch(query: String): Iterator[Result] = {
    new Iterator[Result] {
      val matches = documentBuffer.regexSearch(query).iterator()

      override def hasNext: Boolean = matches.hasNext

      override def next(): Result = {
        val m = matches.next()
        val offset = m.getOffset.toInt
        val recordId = documentBuffer.offsetToRecordId(offset)
        val key = keys(recordId)
        val begin = offset - documentBuffer.getRecordOffset(recordId)
        val end = begin + m.getLength
        Result(key, begin, end, null)
      }
    }
  }

  /**
    * Filter annotations in this partition by the annotation class, annotation type and the
    * annotation metadata.
    *
    * @param annotClassFilter Regex filter on annotation class.
    * @param annotTypeFilter  Regex filter on annotation type.
    * @param metadataFilter   Arbitrary filter function on metadata.
    * @return An [[Iterator]] over the filtered annotations encapsulated as Result objects.
    */
  def filterAnnotations(annotClassFilter: String, annotTypeFilter: String,
                        metadataFilter: String => Boolean): Iterator[Result] = {
    val delim = "\\" + SuccinctAnnotationBuffer.DELIM
    val keyFilter = delim + annotClassFilter + delim + annotTypeFilter + delim
    annotBufferMap.filterKeys(_ matches keyFilter).values.map(_.iterator().asScala)
      .foldLeft(Iterator[Annotation]())(_ ++ _)
      .filter(a => metadataFilter(a.getMetadata))
      .map(a => Result(a.getDocId, a.getStartOffset, a.getEndOffset, a))
  }

  /**
    * Generic method for handling A op B operations where A is FilterAnnotation, and B is any
    * operation
    *
    * @param annotClassFilter Regex filter on annotation class.
    * @param annotTypeFilter  Regex filter on annotation type.
    * @param metadataFilter   Arbitrary filter function on metadata.
    * @param it               Result of operation B
    * @param op               The operation on A and B
    * @return An Iterator over annotations encapsulated in Result objects.
    */
  def annotationsOp(annotClassFilter: String, annotTypeFilter: String,
                    metadataFilter: String => Boolean, it: Iterator[Result],
                    op: (AnnotationRecord, Int, Int) => Array[Annotation]): Iterator[Result] = {
    val delim = "\\" + SuccinctAnnotationBuffer.DELIM
    val keyFilter = delim + annotClassFilter + delim + annotTypeFilter + delim
    val buffers = annotBufferMap.filterKeys(_ matches keyFilter).values.toSeq

    new Iterator[Result] {
      var curBufIdx = buffers.length - 1
      var curAnnotIdx = 0
      var curRes: Result = null
      var curAnnots = nextAnnots

      def nextAnnots: Array[Annotation] = {
        var annots: Array[Annotation] = Array[Annotation]()
        while (annots.length == 0) {
          var annotRecord: AnnotationRecord = null
          while (annotRecord == null) {
            curBufIdx += 1
            if (curBufIdx == buffers.size) {
              curBufIdx = 0
              curRes = if (it.hasNext) it.next() else null
              if (!hasNext) return null
            }
            annotRecord = buffers(curBufIdx).getAnnotationRecord(curRes.docId)
          }
          annots = op(annotRecord, curRes.startOffset, curRes.endOffset)
        }
        annots
      }

      override def hasNext: Boolean = curRes != null

      override def next(): Result = {
        if (!hasNext)
          throw new NoSuchElementException()
        val annot = curAnnots(curAnnotIdx)
        curAnnotIdx += 1
        if (curAnnotIdx == curAnnots.length) {
          curAnnotIdx = 0
          curAnnots = nextAnnots
        }
        Result(annot.getDocId, annot.getStartOffset, annot.getEndOffset, annot)
      }
    }.filter(r => metadataFilter(r.annotation.getMetadata))
  }

  /**
    * Get all filtered annotations that contain a given set of (docId, startOffset, endOffset)
    * triplets.
    *
    * @param annotClassFilter Regex filter on annotation class.
    * @param annotTypeFilter  Regex filter on annotation type.
    * @param metadataFilter   Arbitrary filter function on metadata.
    * @param it               An iterator over the (docId, startOffset, endOffset) triplets
    *                         encapsulated in Result objects.
    * @return An iterator over the matching annotations encapsulated in Result objects.
    */
  def annotationsContainingOp(annotClassFilter: String, annotTypeFilter: String,
                              metadataFilter: String => Boolean,
                              it: Iterator[Result]): Iterator[Result] = {
    annotationsOp(annotClassFilter, annotTypeFilter, metadataFilter, it,
      (a, start, end) => a.annotationsContaining(start, end))
  }

  /**
    * Get all filtered annotations contained in a given set of (docId, startOffset, endOffset)
    * triplets.
    *
    * @param annotClassFilter Regex filter on annotation class.
    * @param annotTypeFilter  Regex filter on annotation type.
    * @param metadataFilter   Arbitrary filter function on metadata.
    * @param it               An iterator over the (docId, startOffset, endOffset) triplets
    *                         encapsulated in Result objects.
    * @return An iterator over the matching annotations encapsulated in Result objects.
    */
  def annotationsContainedInOp(annotClassFilter: String, annotTypeFilter: String,
                               metadataFilter: String => Boolean,
                               it: Iterator[Result]): Iterator[Result] = {
    annotationsOp(annotClassFilter, annotTypeFilter, metadataFilter, it,
      (a, start, end) => a.annotationsContainedIn(start, end))
  }

  /**
    * Get all annotations that occur before a given set of (docId, startOffset, endOffset) triplets.
    *
    * @param annotClassFilter Regex filter on annotation class.
    * @param annotTypeFilter  Regex filter on annotation type.
    * @param metadataFilter   Arbitrary filter function on metadata.
    * @param it               An iterator over the (docId, startOffset, endOffset) triplets
    *                         encapsulated in Result objects.
    * @param range            Max number of chars the annotation can be away from begin; -1 sets the limit
    *                         to infinity, i.e., all annotations before.
    * @return An iterator over the matching annotations encapsulated in Result objects.
    */
  def annotationsBeforeOp(annotClassFilter: String, annotTypeFilter: String,
                          metadataFilter: String => Boolean,
                          it: Iterator[Result], range: Int): Iterator[Result] = {
    annotationsOp(annotClassFilter, annotTypeFilter, metadataFilter, it,
      (a, start, end) => a.annotationsBefore(start, end, range))
  }

  /**
    * Get all annotations that occur before a given set of (docId, startOffset, endOffset) triplets.
    *
    * @param annotClassFilter Regex filter on annotation class.
    * @param annotTypeFilter  Regex filter on annotation type.
    * @param metadataFilter   Arbitrary filter function on metadata.
    * @param it               An iterator over the (docId, startOffset, endOffset) triplets
    *                         encapsulated in Result objects.
    * @param range            Max number of chars the annotation can be away from begin; -1 sets the
    *                         limit to infinity, i.e., all annotations before.
    * @return An iterator over the matching annotations encapsulated in Result objects.
    */
  def annotationsAfterOp(annotClassFilter: String, annotTypeFilter: String,
                         metadataFilter: String => Boolean,
                         it: Iterator[Result], range: Int): Iterator[Result] = {
    annotationsOp(annotClassFilter, annotTypeFilter, metadataFilter, it,
      (a, start, end) => a.annotationsAfter(start, end, range))
  }

  /**
    * Generic method for handling A op B operations where B is FilterAnnotation, and A is any
    * operation.
    *
    * @param it               Result of operation A.
    * @param annotClassFilter Regex filter on annotation class.
    * @param annotTypeFilter  Regex filter on annotation type.
    * @param op               The operation on A and B
    * @return An Iterator over annotations encapsulated in Result objects.
    */
  def opAnnotations(it: Iterator[Result], annotClassFilter: String, annotTypeFilter: String,
                    op: (AnnotationRecord, Int, Int) => Boolean): Iterator[Result] = {
    val delim = "\\" + SuccinctAnnotationBuffer.DELIM
    val keyFilter = delim + annotClassFilter + delim + annotTypeFilter + delim
    val buffers = annotBufferMap.filterKeys(_ matches keyFilter).values.toSeq

    new Iterator[Result] {
      var curBufIdx = buffers.length - 1
      var curRes: Result = nextRes

      def nextRes: Result = {
        var valid: Boolean = false
        while (!valid) {
          var annotRecord: AnnotationRecord = null
          while (annotRecord == null) {
            curBufIdx += 1
            if (curBufIdx == buffers.size) {
              curBufIdx = 0
              curRes = if (it.hasNext) it.next() else null
              if (!hasNext) return null
            }
            annotRecord = buffers(curBufIdx).getAnnotationRecord(curRes.docId)
          }
          valid = op(annotRecord, curRes.startOffset, curRes.endOffset)
        }
        curRes
      }

      override def hasNext: Boolean = curRes != null

      override def next(): Result = {
        if (!hasNext)
          throw new NoSuchElementException()

        val toReturn = curRes
        curRes = nextRes
        toReturn
      }
    }
  }

  /**
    * Get a new instance of MetadataFilter object which wraps an input lambda filter function.
    *
    * @param mFilter The input lambda filter function.
    * @return A MetadataFilter object.
    */
  def newMetadataFilter(mFilter: String => Boolean): MetadataFilter = {
    class MetadataFilterWrapper extends MetadataFilter {
      override def filter(metadata: String): Boolean = mFilter(metadata)
    }
    new MetadataFilterWrapper
  }

  /**
    * Get all (docId, startOffset, endOffset) triplets that contain certain filtered annotations.
    *
    * @param it               An iterator over the (docId, startOffset, endOffset) triplets
    *                         encapsulated in Result objects.
    * @param annotClassFilter Regex filter on annotation class.
    * @param annotTypeFilter  Regex filter on annotation type.
    * @param metadataFilter   Arbitrary filter function on metadata.
    * @return An iterator over the matching (docId, startOffset, endOffset) triplets encapsulated
    *         in Result objects.
    */
  def opContainingAnnotations(it: Iterator[Result], annotClassFilter: String,
                              annotTypeFilter: String,
                              metadataFilter: String => Boolean): Iterator[Result] = {
    // Use the inverse containedIn function to check if annotation is contained in the results.
    opAnnotations(it, annotClassFilter, annotTypeFilter,
      (a, start, end) => a.containedIn(start, end, newMetadataFilter(metadataFilter)))
  }

  /**
    * Get all (docId, startOffset, endOffset) triplets that are contained in certain filtered
    * annotations.
    *
    * @param it               An iterator over the (docId, startOffset, endOffset) triplets
    *                         encapsulated in Result objects.
    * @param annotClassFilter Regex filter on annotation class.
    * @param annotTypeFilter  Regex filter on annotation type.
    * @param metadataFilter   Arbitrary filter function on metadata.
    * @return An iterator over the matching (docId, startOffset, endOffset) triplets encapsulated
    *         in Result objects.
    */
  def opContainedInAnnotations(it: Iterator[Result], annotClassFilter: String,
                               annotTypeFilter: String,
                               metadataFilter: String => Boolean): Iterator[Result] = {
    // Use the inverse containing function to check if annotations contain the results.
    opAnnotations(it, annotClassFilter, annotTypeFilter,
      (a, start, end) => a.contains(start, end, newMetadataFilter(metadataFilter)))
  }

  /**
    * Get all (docId, startOffset, endOffset) triplets that are before certain filtered annotations.
    *
    * @param it               An iterator over the (docId, startOffset, endOffset) triplets
    *                         encapsulated in Result objects.
    * @param annotClassFilter Regex filter on annotation class.
    * @param annotTypeFilter  Regex filter on annotation type.
    * @param metadataFilter   Arbitrary filter function on metadata.
    * @param range            Max number of chars the annotation can be away from begin; -1 sets the
    *                         limit to infinity, i.e., all annotations before.
    * @return An iterator over the matching (docId, startOffset, endOffset) triplets encapsulated
    *         in Result objects.
    */
  def opBeforeAnnotations(it: Iterator[Result], annotClassFilter: String,
                          annotTypeFilter: String,
                          metadataFilter: String => Boolean, range: Int): Iterator[Result] = {
    // Use the inverse containing function to check if annotations occur after the results.
    opAnnotations(it, annotClassFilter, annotTypeFilter,
      (a, start, end) => a.after(start, end, range, newMetadataFilter(metadataFilter)))
  }

  /**
    * Get all (docId, startOffset, endOffset) triplets that are after certain filtered annotations.
    *
    * @param it               An iterator over the (docId, startOffset, endOffset) triplets
    *                         encapsulated in Result objects.
    * @param annotClassFilter Regex filter on annotation class.
    * @param annotTypeFilter  Regex filter on annotation type.
    * @param metadataFilter   Arbitrary filter function on metadata.
    * @param range            Max number of chars the annotation can be away from end; -1 sets the
    *                         limit to infinity, i.e., all annotations after.
    * @return An iterator over the matching (docId, startOffset, endOffset) triplets encapsulated
    *         in Result objects.
    */
  def opAfterAnnotations(it: Iterator[Result], annotClassFilter: String,
                         annotTypeFilter: String,
                         metadataFilter: String => Boolean, range: Int): Iterator[Result] = {
    // Use the inverse containing function to check if annotations occur before the results.
    opAnnotations(it, annotClassFilter, annotTypeFilter,
      (a, start, end) => a.before(start, end, range, newMetadataFilter(metadataFilter)))
  }

  /**
    * Generic binary operation A op B, where A and B are results of arbitrary operations.
    *
    * @param it1       Result of operation A.
    * @param it2       Result of operation B.
    * @param condition The condition which must be satisfied for operation A op B.
    * @return An iterator over matching results.
    */
  def binaryOp(it1: Iterator[Result], it2: Iterator[Result],
               condition: (Result, Result) => Boolean): Iterator[Result] = {
    // TODO: We can optimize if both iterators are sorted
    new Iterator[Result]() {
      val it2Stream = it2.toStream
      var curRes: Result = nextRes

      def nextRes: Result = {
        var valid: Boolean = false
        while (!valid) {
          curRes = if (it1.hasNext) it1.next() else return null
          valid = checkCondition
        }
        curRes
      }

      def checkCondition: Boolean = {
        for (r <- it2Stream) {
          if (condition(curRes, r)) return true
        }
        false
      }

      override def hasNext: Boolean = curRes != null

      override def next(): Result = {
        if (!hasNext)
          throw new NoSuchElementException()

        val toReturn = curRes
        curRes = nextRes
        toReturn
      }
    }
  }

  /**
    * Get all results that contain certain other results.
    *
    * @param it1 First operand.
    * @param it2 Second operand.
    * @return Iterator over matching results.
    */
  def opContainingOp(it1: Iterator[Result], it2: Iterator[Result]): Iterator[Result] = {
    binaryOp(it1, it2, (r1, r2) => r1.startOffset <= r2.startOffset && r1.endOffset >= r2.endOffset)
  }

  /**
    * Get all results that are contained in certain other results.
    *
    * @param it1 First operand.
    * @param it2 Second operand.
    * @return Iterator over matching results.
    */
  def opContainedInOp(it1: Iterator[Result], it2: Iterator[Result]): Iterator[Result] = {
    binaryOp(it1, it2, (r1, r2) => r1.startOffset >= r2.startOffset && r1.endOffset <= r2.endOffset)
  }

  /**
    * Get all results that are before certain other results.
    *
    * @param it1 First operand.
    * @param it2 Second operand.
    * @return Iterator over matching results.
    */
  def opBeforeOp(it1: Iterator[Result], it2: Iterator[Result], range: Int): Iterator[Result] = {
    binaryOp(it1, it2, (r1, r2) => r1.endOffset <= r2.startOffset && !(range != -1 && r2.startOffset - r1.endOffset > range))
  }

  /**
    * Get all results that are after certain other results.
    *
    * @param it1 First operand.
    * @param it2 Second operand.
    * @return Iterator over matching results.
    */
  def opAfterOp(it1: Iterator[Result], it2: Iterator[Result], range: Int): Iterator[Result] = {
    binaryOp(it1, it2, (r1, r2) => r1.startOffset >= r2.endOffset && !(range != -1 && r1.startOffset - r2.endOffset > range))
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
  def query(operator: Operator): Iterator[Result] = {
    operator match {
      case Search(query) => search(query)
      case Regex(query) => regexSearch(query)
      case FilterAnnotations(acFilter, atFilter, mFilter) =>
        filterAnnotations(acFilter, atFilter, mFilter)
      case Contains(a, b) =>
        (a, b) match {
          case (FilterAnnotations(acFilter, atFilter, mFilter), _) =>
            annotationsContainingOp(acFilter, atFilter, mFilter, query(b))
          case (_, FilterAnnotations(acFilter, atFilter, mFilter)) =>
            opContainingAnnotations(query(a), acFilter, atFilter, mFilter)
          case _ => opContainingOp(query(a), query(b))
        }
      case ContainedIn(a, b) =>
        (a, b) match {
          case (FilterAnnotations(acFilter, atFilter, mFilter), _) =>
            annotationsContainedInOp(acFilter, atFilter, mFilter, query(b))
          case (_, FilterAnnotations(acFilter, atFilter, mFilter)) =>
            opContainedInAnnotations(query(a), acFilter, atFilter, mFilter)
          case _ => opContainedInOp(query(a), query(b))
        }
      case Before(a, b, range) =>
        (a, b) match {
          case (FilterAnnotations(acFilter, atFilter, mFilter), _) =>
            annotationsBeforeOp(acFilter, atFilter, mFilter, query(b), range)
          case (_, FilterAnnotations(acFilter, atFilter, mFilter)) =>
            opBeforeAnnotations(query(a), acFilter, atFilter, mFilter, range)
          case _ => opBeforeOp(query(a), query(b), range)
        }
      case After(a, b, range) =>
        (a, b) match {
          case (FilterAnnotations(acFilter, atFilter, mFilter), _) =>
            annotationsAfterOp(acFilter, atFilter, mFilter, query(b), range)
          case (_, FilterAnnotations(acFilter, atFilter, mFilter)) =>
            opAfterAnnotations(query(a), acFilter, atFilter, mFilter, range)
          case _ => opAfterOp(query(a), query(b), range)
        }
      case unknown => throw new UnsupportedOperationException(s"Operation $unknown not supported.")
    }
  }

  /**
    * Get the number of documents in the partition.
    *
    * @return The number of documents in the partition.
    */
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
