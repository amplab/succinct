package edu.berkeley.cs.succinct.sql

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

private[succinct] object SuccinctUtils {
  /** Serialize an object using Java serialization */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

  /** Serialize an object to file using Java serialization */
  def writeObjectToFS[T](conf: Configuration, location: String, o: T) = {
    val path = new Path(location)
    val fs = FileSystem.get(path.toUri, conf)
    val oos = new ObjectOutputStream(fs.create(path))
    oos.writeObject(o)
    oos.close()
  }

  /** Deserialize an object from file using Java serialization */
  def readObjectFromFS[T](conf: Configuration, location: String): T = {
    val path = new Path(location)
    val fs = FileSystem.get(path.toUri, conf)
    val ois = new ObjectInputStream(fs.open(path))
    ois.readObject.asInstanceOf[T]
  }
}
