package edu.berkeley.cs.succinct.block

import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.text.Normalizer

import scala.collection.mutable.ArrayBuffer

class AnnotatedDocumentSerializer extends Serializable {

  val docIds: ArrayBuffer[String] = new ArrayBuffer[String]
  var curDocTextOffset: Int = 0
  val docTextOffsets: ArrayBuffer[Int] = new ArrayBuffer[Int]
  val docTextOS: ByteArrayOutputStream = new ByteArrayOutputStream
  val docAnnotationOS: ByteArrayOutputStream = new ByteArrayOutputStream

  def getDocIds: Array[String] = docIds.toArray

  def getTextBuffer: (Array[Int], Array[Byte]) = (docTextOffsets.toArray, docTextOS.toByteArray)

  def getAnnotationBuffer: Array[Byte] = docAnnotationOS.toByteArray

  def normalize(input: String): String = {
    Normalizer.normalize(input, Normalizer.Form.NFD).replaceAll("[^\\x00-\\x7F]", "*")
  }

  def serialize(it: Iterator[(String, String, String)]): Unit = {
    it.foreach(v => addAnnotatedDocument(v._1, v._2, v._3))
  }

  def addAnnotatedDocument(docId: String, docText: String, docAnnot: String): Unit = {
    docIds += docId
    docTextOffsets += curDocTextOffset
    val buf = docText.getBytes()
    docTextOS.write(buf)
    docTextOS.write('\n'.toByte)
    curDocTextOffset += (buf.length + 1)
    serializeAnnotation(docId, docAnnot)
  }

  def serializeAnnotation(docId: String, docAnnot: String): Unit = {
    val out = new DataOutputStream(docAnnotationOS)
    docAnnot.split('\n').map(annot => annot.split("\\^", 6))
      .map(e => ((docId, e(1), e(2)), (e(0).toInt, e(3).toInt, e(4).toInt, if(e.length == 6) e(5) else "")))
      .groupBy(_._1).mapValues(v => serializeAnnotationEntry(v.map(_._2).sortBy(_._2)))
      .foreach(kv => {
        out.writeByte(AnnotatedDocumentSerializer.DELIM)
        out.writeBytes(kv._1._2)
        out.writeByte(AnnotatedDocumentSerializer.DELIM)
        out.writeBytes(kv._1._3)
        out.writeByte(AnnotatedDocumentSerializer.DELIM)
        out.writeBytes(kv._1._1)
        out.writeByte(AnnotatedDocumentSerializer.DELIM)
        out.write(kv._2)
      })
    out.flush()
  }

  def serializeAnnotationEntry(dat: Array[(Int, Int, Int, String)]): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val out = new DataOutputStream(baos)
    out.writeInt(dat.length)
    dat.map(_._2).foreach(i => out.writeInt(i))
    dat.map(_._3).foreach(i => out.writeInt(i))
    dat.map(_._1).foreach(i => out.writeInt(i))
    dat.map(_._4).foreach(i => {
      out.writeShort(i.length)
      out.writeBytes(i)
    })
    out.flush()
    baos.toByteArray
  }

}

object AnnotatedDocumentSerializer {
  val DELIM: Byte = '^'.toByte
}
