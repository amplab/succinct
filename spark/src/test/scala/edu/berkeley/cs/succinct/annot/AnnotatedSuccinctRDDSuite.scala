package edu.berkeley.cs.succinct.annot

import com.google.common.io.Files
import edu.berkeley.cs.succinct.LocalSparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class AnnotatedSuccinctRDDSuite extends FunSuite with LocalSparkContext {

  val conf = new SparkConf().setAppName("test").setMaster("local")
    .set("spark.driver.allowMultipleContexts", "true")

  val doc1 = ("doc1", "Document number one",
    "1^ge^word^0^8^foo\n2^ge^space^8^9\n3^ge^word^9^15^bar\n4^ge^space^15^16\n5^ge^word^16^19^baz")
  val doc2 = ("doc2", "Document number two",
    "1^ge^word^0^8\n2^ge^space^8^9\n3^ge^word^9^15\n4^ge^space^15^16\n5^ge^word^16^19")
  val doc3 = ("doc3", "Document number three",
    "1^ge^word^0^8^a\n2^ge^space^8^9\n3^ge^word^9^15^b&c\n4^ge^space^15^16\n5^ge^word^16^21^d^e")
  val data: Seq[(String, String, String)] = Seq(doc1, doc2, doc3)
  val docMap: Map[String, String] = data.map(d => (d._1, d._2)).toMap

  test("Test getDocument") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    // Check
    data.map(_._1).foreach(docId => {
      val docText = annotatedSuccinctRDD.getDocument(docId)
      assert(docText == docMap(docId))
    })
  }

  test("Test search") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    // Check
    val res1 = annotatedSuccinctRDD.search("Document").collect()
    assert(res1.sorted === Array[(String, Int, Int)](("doc1", 0, 8), ("doc2", 0, 8), ("doc3", 0, 8)))

    val res2 = annotatedSuccinctRDD.search("number").collect()
    assert(res2.sorted === Array[(String, Int, Int)](("doc1", 9, 15), ("doc2", 9, 15), ("doc3", 9, 15)))

    val res3 = annotatedSuccinctRDD.search("three").collect()
    assert(res3 === Array[(String, Int, Int)](("doc3", 16, 21)))

    val res4 = annotatedSuccinctRDD.search("four").collect()
    assert(res4 === Array[(String, Int, Int)]())
  }

  test("Test regexSearch") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    // Check
    val res1 = annotatedSuccinctRDD.regexSearch("one|two").collect()
    assert(res1.sorted === Array[(String, Int, Int)](("doc1", 16, 19), ("doc2", 16, 19)))

    val res2 = annotatedSuccinctRDD.regexSearch("two|three").collect()
    assert(res2.sorted === Array[(String, Int, Int)](("doc2", 16, 19), ("doc3", 16, 21)))

    val res3 = annotatedSuccinctRDD.search("four|five|six").collect()
    assert(res3 === Array[(String, Int, Int)]())
  }

  test("Test searchOver") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    // Check
    val res1 = annotatedSuccinctRDD.searchOver("Document", "ge", "word").collect()
    assert(res1.length == 3)
    res1.foreach(a => {
      assert(a.getrBegin() == 0)
      assert(a.getrEnd() == 8)
      assert(a.getId == 1)
    })

    val res2 = annotatedSuccinctRDD.searchOver("number", "ge", "word").collect()
    assert(res2.length == 3)
    res2.foreach(a => {
      assert(a.getrBegin() == 9)
      assert(a.getrEnd() == 15)
      assert(a.getId == 3)
    })

    val res3 = annotatedSuccinctRDD.searchOver("three", "ge", "word").collect()
    assert(res3.length == 1)
    assert(res3(0).getId == 5)
    assert(res3(0).getrBegin() == 16)
    assert(res3(0).getrEnd() == 21)
    assert(res3(0).getMetadata == "d^e")

    val res4 = annotatedSuccinctRDD.searchOver(" ", "ge", "space").collect()
    assert(res4.length == 6)
    res4.foreach(a => {
      assert(a.getId == 2 || a.getId == 4)
      assert(a.getrBegin() == 8 || a.getrBegin() == 15)
      assert(a.getrEnd() == 9 || a.getrEnd() == 16)
      assert(a.getMetadata == "")
    })

    val res5 = annotatedSuccinctRDD.searchOver("four", "ge", "word").collect()
    assert(res5.length == 0)
  }

  test("Test regexOver") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    // Check
    val res1 = annotatedSuccinctRDD.regexOver("one|two|three", "ge", "word").collect()
    assert(res1.length == 3)
    res1.foreach(a => {
      assert(a.getrBegin() == 16)
      assert(a.getrEnd() == 19 | a.getrEnd() == 21)
      assert(a.getId == 5)
    })

    val res2 = annotatedSuccinctRDD.regexOver("four|five|six", "ge", "word").collect()
    assert(res2.length == 0)
  }

  test("Test save and load") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct"
    annotatedSuccinctRDD.save(succinctDir)

    val reloadedRDD = AnnotatedSuccinctRDD(sc, succinctDir)

    val originalData = annotatedSuccinctRDD.collect()
    val newData = reloadedRDD.collect()

    assert(originalData === newData)
  }
}
