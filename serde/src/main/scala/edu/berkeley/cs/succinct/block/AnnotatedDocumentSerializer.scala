package edu.berkeley.cs.succinct.block

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.net.URLDecoder
import java.util.InvalidPropertiesFormatException

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

class AnnotatedDocumentSerializer(ignoreParseErrors: Boolean) extends Serializable {

  val docIds: ArrayBuffer[String] = new ArrayBuffer[String]
  var curDocTextOffset: Int = 0
  val docTextOffsets: ArrayBuffer[Int] = new ArrayBuffer[Int]
  val docTextOS: StringBuilder = new StringBuilder
  var docAnnotDataMap: Map[String, (ArrayBuffer[Int], ArrayBuffer[Int], ByteArrayOutputStream)] = {
    new TreeMap[String, (ArrayBuffer[Int], ArrayBuffer[Int], ByteArrayOutputStream)]()
  }

  def getDocIds: Array[String] = docIds.toArray

  def getTextBuffer: (Array[Int], Array[Char]) = (docTextOffsets.toArray, docTextOS.toArray)

  def getAnnotationBuffers: Map[String, (Array[Int], Array[Int], Array[Byte])] = {
    docAnnotDataMap.mapValues(aData => (aData._1.toArray, aData._2.toArray, aData._3.toByteArray))
  }

  def serialize(it: Iterator[(String, String, String)]): Unit = {
    it.foreach(v => addAnnotatedDocument(v._1, v._2, v._3))
  }

  def makeKey(annotClass: String, annotType: String): String = {
    val delim = AnnotatedDocumentSerializer.DELIM
    delim + annotClass + delim + annotType + delim
  }

  def addAnnotation(docIdOffset: Int, docAnnotation: String): Unit = {
    docAnnotation.split('\n').map(annot => annot.split("\\^", 6))
      .map(e => (makeKey(e(1), e(2)), (e(0).toInt, e(3).toInt, e(4).toInt, if (e.length == 6) URLDecoder.decode(e(5), "UTF-8") else "")))
      .groupBy(_._1).mapValues(v => serializeAnnotationEntry(v.map(_._2).sortBy(_._2)))
      .foreach(kv => {
        if (!docAnnotDataMap.contains(kv._1))
          docAnnotDataMap += (kv._1 -> (new ArrayBuffer[Int], new ArrayBuffer[Int], new ByteArrayOutputStream()))
        val annotData = docAnnotDataMap(kv._1)
        annotData._1.append(docIdOffset)
        annotData._2.append(annotData._3.size())
        annotData._3.write(kv._2)
      })
  }

  def addAnnotatedDocument(docId: String, docText: String, docAnnot: String): Unit = {
    val docIdOffset = docIds.length
    docIds += docId
    docTextOffsets += curDocTextOffset
    docTextOS.append(docText)
    docTextOS.append('\n')
    curDocTextOffset += (docText.length + 1)
    addAnnotation(docIdOffset, docAnnot)
  }

  def serializeAnnotationEntry(dat: Array[(Int, Int, Int, String)]): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val out = new DataOutputStream(baos)

    out.writeInt(dat.length)
    dat.map(_._2).foreach(i => out.writeInt(i))
    dat.map(_._3).foreach(i => out.writeInt(i))
    dat.map(_._1).foreach(i => out.writeInt(i))
    dat.map(_._4).foreach(i => {
      if (i.length > Short.MaxValue && !ignoreParseErrors)
        throw new InvalidPropertiesFormatException("Metadata too large: " + i.length + "; limit: " + Short.MaxValue)
      val metadata = i.substring(0, Math.min(Short.MaxValue, i.length))
      out.writeShort(metadata.length)
      out.writeBytes(metadata)
    })
    out.flush()
    baos.toByteArray
  }

}

object AnnotatedDocumentSerializer {
  val DELIM: Char = '^'
}
