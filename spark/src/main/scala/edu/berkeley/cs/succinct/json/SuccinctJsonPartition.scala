package edu.berkeley.cs.succinct.json

import java.io.{ObjectInputStream, ObjectOutputStream, DataOutputStream}

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

  override private[succinct] def iterator: Iterator[String] = {
    new Iterator[String] {
      var curRecordId = 0

      override def hasNext: Boolean = curRecordId < ids.length

      override def next(): String = jsonDeserializer.deserialize(valueBuffer.getRecord(curRecordId))
    }
  }

  private[succinct] def jGet(id: Long): String = {
    val doc = get(id)
    if (doc == null) null else jsonDeserializer.deserialize(doc)
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
