package edu.berkeley.cs.succinct.kv

import java.io.{DataOutputStream, ObjectInputStream, ObjectOutputStream}

import edu.berkeley.cs.succinct.SuccinctIndexedFile
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.streams.SuccinctIndexedFileStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * The implementation for a single SuccinctKVRDD partition.
 */
class SuccinctKVPartition[K: ClassTag](keys: Array[K], valueBuffer: SuccinctIndexedFile)(implicit ordering: Ordering[K]) {

  val numKeys = keys.length

  private[kv] def iterator: Iterator[(K, Array[Byte])] = {
    new Iterator[(K, Array[Byte])] {
      var currentRecordId = 0

      override def hasNext: Boolean = currentRecordId < keys.length

      override def next(): (K, Array[Byte]) = {
        val curKey = keys(currentRecordId)
        val curVal = valueBuffer.getRecord(currentRecordId)
        currentRecordId += 1
        (curKey, curVal)
      }
    }
  }

  /** Find the index of a particular key using binary search. **/
  private[kv] def findKey(key: K): Int = {
    var (low, high) = (0, numKeys - 1)

    while (low <= high)
      (low + high) / 2 match {
        case mid if ordering.gt(keys(mid), key) => high = mid - 1
        case mid if ordering.lt(keys(mid), key) => low = mid + 1
        case mid => return mid
      }
    -1
  }

  /** Get the value corresponding to a key. **/
  private[kv] def get(key: K): Array[Byte] = {
    val pos = findKey(key)
    if (pos < 0 || pos > numKeys) null else valueBuffer.getRecord(pos)
  }

  /** Random access into the value corresponding to a key. **/
  private[kv] def access(key: K, offset: Int, length: Int): Array[Byte] = {
    val pos = findKey(key)
    if (pos < 0 || pos > numKeys) null else valueBuffer.accessRecord(pos, offset, length)
  }

  /** Search across values, and return all keys for matched values. **/
  private[kv] def search(query: Array[Byte]): Iterator[K] = {
    new Iterator[K] {
      val recordIds = valueBuffer.recordSearchIdIterator(query)

      override def hasNext: Boolean = recordIds.hasNext

      override def next(): K = keys(recordIds.next)
    }
  }

  /** Get the number of KV pairs in this partition. **/
  private[kv] def count: Long = numKeys

  /** Write the partition data to output stream. **/
  private[kv] def writeToStream(dataStream: DataOutputStream) = {
    valueBuffer.writeToStream(dataStream)
    val objectOutputStream = new ObjectOutputStream(dataStream)
    objectOutputStream.writeObject(keys)
  }

}

object SuccinctKVPartition {
  def apply[K: ClassTag](
    partitionLocation: String, storageLevel: StorageLevel)
    (implicit ordering: Ordering[K])
  : SuccinctKVPartition[K] = {

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

    new SuccinctKVPartition[K](keys, valueBuffer)
  }
}
