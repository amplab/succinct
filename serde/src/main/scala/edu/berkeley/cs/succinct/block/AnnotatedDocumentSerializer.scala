package edu.berkeley.cs.succinct.block

import java.io.{ByteArrayOutputStream, DataOutputStream}

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

class AnnotatedDocumentSerializer extends Serializable {

  val docIds: ArrayBuffer[String] = new ArrayBuffer[String]
  var curDocTextOffset: Int = 0
  val docTextOffsets: ArrayBuffer[Int] = new ArrayBuffer[Int]
  val docTextOS: StringBuilder = new StringBuilder
  var docAnnotationOSMap: Map[String, ByteArrayOutputStream] = new TreeMap[String, ByteArrayOutputStream]()

  def getDocIds: Array[String] = docIds.toArray

  def getTextBuffer: (Array[Int], Array[Char]) = (docTextOffsets.toArray, docTextOS.toArray)

  def getAnnotationBuffers: Map[String, Array[Byte]] = docAnnotationOSMap.mapValues(_.toByteArray)

  def serialize(it: Iterator[(String, String, String)]): Unit = {
    it.foreach(v => addAnnotatedDocument(v._1, v._2, v._3))
  }

  def makeKey(annotClass: String, annotType: String): String = {
    val delim = AnnotatedDocumentSerializer.DELIM
    delim + annotClass + delim + annotType + delim
  }

  def addAnnotation(docId: String, docAnnotation: String): Unit = {
    docAnnotation.split('\n').map(annot => annot.split("\\^", 6))
      .map(e => (makeKey(e(1), e(2)), (e(0).toInt, e(3).toInt, e(4).toInt, if (e.length == 6) e(5) else "")))
      .groupBy(_._1).mapValues(v => serializeAnnotationEntry(docId, v.map(_._2).sortBy(_._2)))
      .foreach(kv => {
        if (!docAnnotationOSMap.contains(kv._1))
          docAnnotationOSMap += (kv._1 -> new ByteArrayOutputStream())
        docAnnotationOSMap(kv._1).write(kv._2)
      })
  }

  def addAnnotatedDocument(docId: String, docText: String, docAnnot: String): Unit = {
    docIds += docId
    docTextOffsets += curDocTextOffset
    docTextOS.append(docText)
    docTextOS.append('\n')
    curDocTextOffset += (docText.length + 1)
    addAnnotation(docId, docAnnot)
  }

  def serializeAnnotationEntry(docId: String, dat: Array[(Int, Int, Int, String)]): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val out = new DataOutputStream(baos)
    out.writeByte(AnnotatedDocumentSerializer.DELIM)
    out.writeBytes(docId)
    out.writeByte(AnnotatedDocumentSerializer.DELIM)
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
  val DELIM: Char = '^'
}
