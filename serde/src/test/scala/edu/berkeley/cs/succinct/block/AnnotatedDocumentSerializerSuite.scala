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
    val ser = new AnnotatedDocumentSerializer
    ser.serialize(data.iterator)

    // Check docIds
    val docIds = ser.getDocIds
    assert(docIds === Array[String]("doc1", "doc2", "doc3"))

    // Check document text
    val (docOffsets, docText) = ser.getTextBuffer
    assert(docOffsets === Array[Int](0, 20, 40))
    assert(docText === (doc1._2 + "\n" + doc2._2 + "\n" + doc3._2 + "\n").getBytes())

    // Check document annotations
    val annotBuffer = ser.getAnnotationBuffer
    val bais = new ByteArrayInputStream(annotBuffer)
    val in = new DataInputStream(bais)

    Seq(1, 2, 3).foreach(i => {
      Seq(0, 1).foreach(d => {
        assert(in.readByte() == DELIM)
        var b = 0.toByte
        var docClass = ""
        b = in.readByte()
        while (b != DELIM) {
          docClass += b.toChar
          b = in.readByte()
        }
        assert(docClass == "ge")
        var docId = ""
        b = in.readByte()
        while (b != DELIM) {
          docId += b.toChar
          b = in.readByte()
        }
        var docType = ""
        b = in.readByte()
        while (b != DELIM) {
          docType += b.toChar
          b = in.readByte()
        }

        val numEntries = in.readInt()
        if (docType == "word") {
          assert(numEntries == 3)

          // Range begins
          assert(in.readInt() == 0)
          assert(in.readInt() == 9)
          assert(in.readInt() == 16)

          // Range ends
          assert(in.readInt() == 8)
          assert(in.readInt() == 15)
          if (docId == "doc3")
            assert(in.readInt() == 21)
          else
            assert(in.readInt() == 19)

          // Annotation Ids
          assert(in.readInt() == 1)
          assert(in.readInt() == 3)
          assert(in.readInt() == 5)

          // Metadata
          if (docId == "doc1") {
            val len1 = in.readShort()
            val buf1 = new Array[Byte](len1)
            in.read(buf1)
            assert(buf1 === "foo".getBytes())

            val len2 = in.readShort()
            val buf2 = new Array[Byte](len2)
            in.read(buf2)
            assert(buf2 === "bar".getBytes())

            val len3 = in.readShort()
            val buf3 = new Array[Byte](len3)
            in.read(buf3)
            assert(buf3 === "baz".getBytes())
          } else if (docId == "doc2") {
            assert(in.readShort() == 0)
            assert(in.readShort() == 0)
            assert(in.readShort() == 0)
          } else if (docId == "doc3") {
            val len1 = in.readShort()
            val buf1 = new Array[Byte](len1)
            in.read(buf1)
            assert(buf1 === "a".getBytes())

            val len2 = in.readShort()
            val buf2 = new Array[Byte](len2)
            in.read(buf2)
            assert(buf2 === "b&c".getBytes())

            val len3 = in.readShort()
            val buf3 = new Array[Byte](len3)
            in.read(buf3)
            assert(buf3 === "d^e".getBytes())
          }
        } else if (docType == "space") {
          assert(numEntries == 2)

          // Range begins
          assert(in.readInt() == 8)
          assert(in.readInt() == 15)

          // Range ends
          assert(in.readInt() == 9)
          assert(in.readInt() == 16)

          // Annotation Ids
          assert(in.readInt() == 2)
          assert(in.readInt() == 4)

          // Metadata
          assert(in.readShort() == 0)
          assert(in.readShort() == 0)
        }
      })
    })
  }
}
