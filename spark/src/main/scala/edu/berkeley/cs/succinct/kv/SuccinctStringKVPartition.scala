package org.apache.spark.succinct.kv

import java.io._
import java.{lang, util}

import edu.berkeley.cs.succinct.SuccinctIndexedFile
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.regex.RegExMatch
import edu.berkeley.cs.succinct.streams.SuccinctIndexedFileStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{KnownSizeEstimation, SizeEstimator}

import scala.reflect.ClassTag

class SuccinctStringKVPartition[K: ClassTag](keys: Array[K], valueBuffer: SuccinctIndexedFile)
                                            (implicit ordering: Ordering[K]) extends KnownSizeEstimation with Serializable {

  val numKeys: Int = keys.length

  override def estimatedSize: Long = valueBuffer.getCompressedSize + SizeEstimator.estimate(keys)

  def iterator: Iterator[(K, String)] = {
    new Iterator[(K, String)] {
      var currentRecordId = 0

      override def hasNext: Boolean = currentRecordId < keys.length

      override def next(): (K, String) = {
        val curKey = keys(currentRecordId)
        val curVal = valueBuffer.getRecord(currentRecordId)
        currentRecordId += 1
        (curKey, curVal)
      }
    }
  }

  /** Get the values corresponding to an array of keys. **/
  def multiget(keys: Array[K]): Array[(K, String)] = {
    keys.map(k => (k, get(k)))
  }

  /** Get the value corresponding to a key. **/
  def get(key: K): String = {
    val pos = findKey(key)
    if (pos < 0 || pos > numKeys) null else valueBuffer.getRecord(pos)
  }

  /** Find the index of a particular key using binary search. **/
  def findKey(key: K): Int = {
    var (low, high) = (0, numKeys - 1)

    while (low <= high)
      (low + high) / 2 match {
        case mid if ordering.gt(keys(mid), key) => high = mid - 1
        case mid if ordering.lt(keys(mid), key) => low = mid + 1
        case mid => return mid
      }
    -1
  }

  /** Random access into the value corresponding to a key. **/
  def extract(key: K, offset: Int, length: Int): String = {
    val pos = findKey(key)
    if (pos < 0 || pos > numKeys) null else valueBuffer.extractRecord(pos, offset, length)
  }

  /** Search across values, and return all keys for matched values. **/
  def search(query: String): Iterator[K] = {
    new Iterator[K] {
      val recordIds: util.Iterator[Integer] = valueBuffer.recordSearchIdIterator(query.toCharArray)

      override def hasNext: Boolean = recordIds.hasNext

      override def next(): K = keys(recordIds.next)
    }
  }

  /** Search across values, and return all (key, value) pairs for matched values. **/
  def searchAndGet(query: String): Iterator[(K, String)] = {
    new Iterator[(K, String)] {
      val recordIds: util.Iterator[Integer] = valueBuffer.recordSearchIdIterator(query.toCharArray)

      override def hasNext: Boolean = recordIds.hasNext

      override def next(): (K, String) = {
        val recordId = recordIds.next()
        (keys(recordId), valueBuffer.getRecord(recordId))
      }
    }
  }

  /** Search across values, and return the number of occurrences of a particular query. **/
  def count(query: String): Long = {
    valueBuffer.count(query.toCharArray)
  }

  /** Search across values, and return the key, match offset (relative to each value) pairs. **/
  def searchOffsets(query: String): Iterator[(K, Int)] = {
    new Iterator[(K, Int)] {
      val searchIterator: util.Iterator[lang.Long] = valueBuffer.searchIterator(query.toCharArray)

      override def hasNext: Boolean = searchIterator.hasNext

      override def next(): (K, Int) = {
        val offset = searchIterator.next().toInt
        val recordId = valueBuffer.offsetToRecordId(offset)
        val key = keys(recordId)
        (key, offset - valueBuffer.getRecordOffset(recordId))
      }
    }
  }

  /** Regex search across values, and return all keys for matched values. **/
  def regexSearch(query: String): Iterator[K] = {
    new Iterator[K] {
      val recordIds: Iterator[Integer] = valueBuffer.recordSearchRegexIds(query).iterator

      override def hasNext: Boolean = recordIds.hasNext

      override def next(): K = keys(recordIds.next())
    }
  }

  /** Regex search across values, and regurn the key, match (relative to each value) pairs. **/
  def regexMatch(query: String): Iterator[(K, RegExMatch)] = {
    new Iterator[(K, RegExMatch)] {
      val matches: util.Iterator[RegExMatch] = valueBuffer.regexSearch(query).iterator()

      override def hasNext: Boolean = matches.hasNext

      override def next(): (K, RegExMatch) = {
        val m = matches.next()
        val offset = m.getOffset.toInt
        val recordId = valueBuffer.offsetToRecordId(offset)
        val key = keys(recordId)
        m.setOffset(offset - valueBuffer.getRecordOffset(recordId))
        (key, m)
      }
    }
  }

  /** Get the number of KV pairs in this partition. **/
  def count: Long = numKeys

  /** Write the partition data to output stream. **/
  def writeToStream(dataStream: DataOutputStream): Unit = {
    valueBuffer.writeToStream(dataStream)
    val objectOutputStream = new ObjectOutputStream(dataStream)
    objectOutputStream.writeObject(keys)
  }

  /** Returns the first key in the partition. **/
  def firstKey: K = keys(0)
}

object SuccinctStringKVPartition {
  def apply[K: ClassTag](partitionLocation: String, storageLevel: StorageLevel)
                        (implicit ordering: Ordering[K])
  : SuccinctStringKVPartition[K] = {

    val path = new Path(partitionLocation)
    val fs = FileSystem.get(path.toUri, new Configuration())
    val is = fs.open(path)
    val valueBuffer = storageLevel match {
      case StorageLevel.MEMORY_ONLY =>
        new SuccinctIndexedFileBuffer(is)
      case StorageLevel.DISK_ONLY =>
        new SuccinctIndexedFileStream(path)
      case _ =>
        new SuccinctIndexedFileBuffer(is)
    }
    val ois = new ObjectInputStream(is)
    val keys = ois.readObject().asInstanceOf[Array[K]]
    is.close()

    new SuccinctStringKVPartition[K](keys, valueBuffer)
  }
}