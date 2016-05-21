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

  test("Test extractDocument") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    // Check
    data.map(_._1).foreach(docId => {
      val docText = annotatedSuccinctRDD.extractDocument(docId, 9, 6)
      assert(docText == "number")
    })
  }

  test("Test search") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    // Check
    val res1 = annotatedSuccinctRDD.search("Document").collect()
    assert(res1 contains Result("doc1", 0, 8, null))
    assert(res1 contains Result("doc2", 0, 8, null))
    assert(res1 contains Result("doc3", 0, 8, null))
    assert(res1.length == 3)

    val res2 = annotatedSuccinctRDD.search("number").collect()
    assert(res2 contains Result("doc1", 9, 15, null))
    assert(res2 contains Result("doc2", 9, 15, null))
    assert(res2 contains Result("doc3", 9, 15, null))
    assert(res2.length == 3)

    val res3 = annotatedSuccinctRDD.search("three").collect()
    assert(res3 contains Result("doc3", 16, 21, null))
    assert(res3.length == 1)

    val res4 = annotatedSuccinctRDD.search("four").collect()
    assert(res4.length == 0)
  }

  test("Test regexSearch") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    // Check
    val res1 = annotatedSuccinctRDD.regexSearch("one|two").collect()
    assert(res1 contains Result("doc1", 16, 19, null))
    assert(res1 contains Result("doc2", 16, 19, null))
    assert(res1.length == 2)

    val res2 = annotatedSuccinctRDD.regexSearch("two|three").collect()
    assert(res2 contains Result("doc2", 16, 19, null))
    assert(res2 contains Result("doc3", 16, 21, null))
    assert(res2.length == 2)

    val res3 = annotatedSuccinctRDD.search("four|five|six").collect()
    assert(res3.length == 0)
  }

  test("Test filterAnnotations") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    val geWords = annotatedSuccinctRDD.filterAnnotations("ge", "word", _ => true).collect()
    assert(geWords.length == 9)
    geWords.foreach(r => {
      assert(r.annotation.getAnnotClass == "ge")
      assert(r.annotation.getAnnotType == "word")
    })

    val geSpaces = annotatedSuccinctRDD.filterAnnotations("ge", "space", _ => true).collect()
    assert(geSpaces.length == 6)
    geSpaces.foreach(r => {
      assert(r.annotation.getAnnotClass == "ge")
      assert(r.annotation.getAnnotType == "space")
    })

    val geAll = annotatedSuccinctRDD.filterAnnotations("ge", ".*", _ => true).collect()
    assert(geAll.length == 15)
    geAll.foreach(a => {
      assert(a.annotation.getAnnotClass == "ge")
      assert(a.annotation.getAnnotType == "word" || a.annotation.getAnnotType == "space")
    })
  }

  test("Test searchContaining") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    // Check
    val res1 = annotatedSuccinctRDD.searchContaining("ge", "word", _ => true, "Document").collect()
    assert(res1.length == 3)
    res1.foreach(a => {
      assert(a.annotation.getStartOffset == 0)
      assert(a.annotation.getEndOffset == 8)
      assert(a.annotation.getId == 1)
    })

    val res2 = annotatedSuccinctRDD.searchContaining("ge", "word", _ => true, "number").collect()
    assert(res2.length == 3)
    res2.foreach(a => {
      assert(a.annotation.getStartOffset == 9)
      assert(a.annotation.getEndOffset == 15)
      assert(a.annotation.getId == 3)
    })

    val res3 = annotatedSuccinctRDD.searchContaining("ge", "word", _ => true, "three").collect()
    assert(res3.length == 1)
    assert(res3(0).annotation.getId == 5)
    assert(res3(0).annotation.getStartOffset == 16)
    assert(res3(0).annotation.getEndOffset == 21)
    assert(res3(0).annotation.getMetadata == "d^e")

    val res4 = annotatedSuccinctRDD.searchContaining("ge", "space", _ => true, " ").collect()
    assert(res4.length == 6)
    res4.foreach(a => {
      assert(a.annotation.getId == 2 || a.annotation.getId == 4)
      assert(a.annotation.getStartOffset == 8 || a.annotation.getStartOffset == 15)
      assert(a.annotation.getEndOffset == 9 || a.annotation.getEndOffset == 16)
      assert(a.annotation.getMetadata == "")
    })

    val res5 = annotatedSuccinctRDD.searchContaining("ge", "word", _ => true, "four").collect()
    assert(res5.length == 0)
  }

  test("Test regexContaining") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    // Check
    val res1 = annotatedSuccinctRDD.regexContaining("ge", "word", _ => true, "one|two|three").collect()
    assert(res1.length == 3)
    res1.foreach(a => {
      assert(a.annotation.getStartOffset == 16)
      assert(a.annotation.getEndOffset == 19 | a.annotation.getEndOffset == 21)
      assert(a.annotation.getId == 5)
    })

    val res2 = annotatedSuccinctRDD.regexContaining("ge", "word", _ => true, "four|five|six").collect()
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
