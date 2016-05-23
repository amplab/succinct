package edu.berkeley.cs.succinct.block

import java.io.{ByteArrayInputStream, DataInputStream}

import org.scalatest.FunSuite

class AnnotatedDocumentSerializerSuite extends FunSuite {

  val DELIM = '^'.toByte
  val doc1 = ("doc1", "Document number one",
    "1^ge^word^0^8^foo\n2^ge^space^8^9\n3^ge^word^9^15^bar\n4^ge^space^15^16\n5^ge^word^16^19^baz")
  val doc2 = ("doc2", "Document number two",
    "1^ge^word^0^8\n2^ge^space^8^9\n3^ge^word^9^15\n4^ge^space^15^16\n5^ge^word^16^19")
  val doc3 = ("doc3", "Document number three",
    "1^ge^word^0^8^a\n2^ge^space^8^9\n3^ge^word^9^15^b&c\n4^ge^space^15^16\n5^ge^word^16^21^d^e")
  val data: Seq[(String, String, String)] = Seq(doc1, doc2, doc3)

  test("serialize") {
    val ser = new AnnotatedDocumentSerializer(true)
    ser.serialize(data.iterator)

    // Check docIds
    val docIds = ser.getDocIds
    assert(docIds === Array[String]("doc1", "doc2", "doc3"))

    // Check document text
    val (docOffsets, docText) = ser.getTextBuffer
    assert(docOffsets === Array[Int](0, 20, 40))
    assert(docText === (doc1._2 + "\n" + doc2._2 + "\n" + doc3._2 + "\n").toCharArray)

    // Check document annotations
    val annotBuffers = ser.getAnnotationBuffers

    val geWordData = annotBuffers("^ge^word^")
    val geWordDocIdOffsets = geWordData._1
    val geWordOffsets = geWordData._2
    assert(geWordOffsets === Array[Int](0, 55, 101))
    val geWordBuffer = geWordData._3

    val geWordBais = new ByteArrayInputStream(geWordBuffer)
    val geWordIn = new DataInputStream(geWordBais)

    val geSpaceData = annotBuffers("^ge^space^")
    val geSpaceDocIdOffsets = geSpaceData._1
    val geSpaceOffsets = geSpaceData._2
    assert(geSpaceOffsets === Array[Int](0, 32, 64))
    val geSpaceBuffer = geSpaceData._3
    val geSpaceBais = new ByteArrayInputStream(geSpaceBuffer)
    val geSpaceIn = new DataInputStream(geSpaceBais)

    Seq(0, 1, 2).foreach(i => {
      {
        val docIdOffset = geWordDocIdOffsets(i)
        assert(docIdOffset == i)

        val numEntries = geWordIn.readInt()
        assert(numEntries == 3)

        // Range begins
        assert(geWordIn.readInt() == 0)
        assert(geWordIn.readInt() == 9)
        assert(geWordIn.readInt() == 16)

        // Range ends
        assert(geWordIn.readInt() == 8)
        assert(geWordIn.readInt() == 15)
        if (docIds(docIdOffset) == "doc3")
          assert(geWordIn.readInt() == 21)
        else
          assert(geWordIn.readInt() == 19)

        // Annotation Ids
        assert(geWordIn.readInt() == 1)
        assert(geWordIn.readInt() == 3)
        assert(geWordIn.readInt() == 5)

        // Metadata
        if (docIds(docIdOffset) == "doc1") {
          val len1 = geWordIn.readShort()
          val buf1 = new Array[Byte](len1)
          geWordIn.read(buf1)
          assert(buf1 === "foo".getBytes())

          val len2 = geWordIn.readShort()
          val buf2 = new Array[Byte](len2)
          geWordIn.read(buf2)
          assert(buf2 === "bar".getBytes())

          val len3 = geWordIn.readShort()
          val buf3 = new Array[Byte](len3)
          geWordIn.read(buf3)
          assert(buf3 === "baz".getBytes())
        } else if (docIds(docIdOffset) == "doc2") {
          assert(geWordIn.readShort() == 0)
          assert(geWordIn.readShort() == 0)
          assert(geWordIn.readShort() == 0)
        } else if (docIds(docIdOffset) == "doc3") {
          val len1 = geWordIn.readShort()
          val buf1 = new Array[Byte](len1)
          geWordIn.read(buf1)
          assert(buf1 === "a".getBytes())

          val len2 = geWordIn.readShort()
          val buf2 = new Array[Byte](len2)
          geWordIn.read(buf2)
          assert(buf2 === "b&c".getBytes())

          val len3 = geWordIn.readShort()
          val buf3 = new Array[Byte](len3)
          geWordIn.read(buf3)
          assert(buf3 === "d^e".getBytes())
        }
      }
      {
        val docIdOffset = geSpaceDocIdOffsets(i)
        assert(docIdOffset == i)

        val numEntries = geSpaceIn.readInt()
        assert(numEntries == 2)

        // Range begins
        assert(geSpaceIn.readInt() == 8)
        assert(geSpaceIn.readInt() == 15)

        // Range ends
        assert(geSpaceIn.readInt() == 9)
        assert(geSpaceIn.readInt() == 16)

        // Annotation Ids
        assert(geSpaceIn.readInt() == 2)
        assert(geSpaceIn.readInt() == 4)

        // Metadata
        assert(geSpaceIn.readShort() == 0)
        assert(geSpaceIn.readShort() == 0)
      }
    })
  }
}
