package edu.berkeley.cs.succinct.json

import java.io.{DataOutputStream, ObjectInputStream, ObjectOutputStream}

import edu.berkeley.cs.succinct.SuccinctIndexedFile
import edu.berkeley.cs.succinct.`object`.deserializer.JsonDeserializer
import edu.berkeley.cs.succinct.block.json.FieldMapping
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import edu.berkeley.cs.succinct.kv.SuccinctKVPartition
import edu.berkeley.cs.succinct.streams.SuccinctIndexedFileStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.storage.StorageLevel

class SuccinctJsonPartition(ids: Array[Long], valueBuffer: SuccinctIndexedFile,
                            fieldMapping: FieldMapping)
  extends SuccinctKVPartition[Long](ids, valueBuffer) {

  val jsonDeserializer: JsonDeserializer = new JsonDeserializer(fieldMapping)

  private[succinct] def jIterator: Iterator[String] = {
    new Iterator[String] {
      var curRecordId = 0

      override def hasNext: Boolean = curRecordId < ids.length

      override def next(): String = {
        val json = jsonDeserializer.deserialize(ids(curRecordId),
          getDocBytes(curRecordId))
        curRecordId += 1
        json
      }
    }
  }

  private[succinct] def getDocBytes(recordId: Int): Array[Byte] = {
    val start = valueBuffer.getRecordOffset(recordId)
    val end = if (recordId == valueBuffer.getNumRecords - 1) valueBuffer.getSize - 1
      else valueBuffer.getRecordOffset(recordId + 1)
    valueBuffer.extract(start, end - start)
  }

  private[succinct] def jGet(id: Long): String = {
    val pos = findKey(id)
    if (pos < 0 || pos > numKeys) null else jsonDeserializer.deserialize(id, getDocBytes(pos))
  }

  private[succinct] def jSearch(field: String, value: String): Iterator[Long] = {
    val delim = fieldMapping.getDelimiter(field)
    val query: Array[Byte] = delim +: value.getBytes :+ delim
    search(query)
  }

  override private[succinct] def writeToStream(dataStream: DataOutputStream): Unit = {
    super.writeToStream(dataStream)
    val objectOutputStream = new ObjectOutputStream(dataStream)
    objectOutputStream.writeObject(fieldMapping)
  }
}

object SuccinctJsonPartition {
  def apply(partitionLocation: String, storageLevel: StorageLevel): SuccinctJsonPartition = {
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
    val ids = ois.readObject().asInstanceOf[Array[Long]]
    val fieldMapping = ois.readObject().asInstanceOf[FieldMapping]
    is.close()

    new SuccinctJsonPartition(ids, valueBuffer, fieldMapping)
  }
}
