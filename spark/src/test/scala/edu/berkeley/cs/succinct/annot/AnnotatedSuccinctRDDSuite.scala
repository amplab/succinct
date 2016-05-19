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

  test("Test filterAnnotations") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    val geWords = annotatedSuccinctRDD.filterAnnotations("ge", "word").collect()
    assert(geWords.length == 9)
    geWords.foreach(a => {
      assert(a.getAnnotClass == "ge")
      assert(a.getAnnotType == "word")
    })

    val geSpaces = annotatedSuccinctRDD.filterAnnotations("ge", "space").collect()
    assert(geSpaces.length == 6)
    geSpaces.foreach(a => {
      assert(a.getAnnotClass == "ge")
      assert(a.getAnnotType == "space")
    })

    val geAll = annotatedSuccinctRDD.filterAnnotations("ge", ".*").collect()
    assert(geAll.length == 15)
    geAll.foreach(a => {
      assert(a.getAnnotClass == "ge")
      assert(a.getAnnotType == "word" || a.getAnnotType == "space")
    })
  }

  test("Test searchContaining") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    // Check
    val res1 = annotatedSuccinctRDD.searchContaining("ge", "word", "Document").collect()
    assert(res1.length == 3)
    res1.foreach(a => {
      assert(a.getStartOffset == 0)
      assert(a.getEndOffset == 8)
      assert(a.getId == 1)
    })

    val res2 = annotatedSuccinctRDD.searchContaining("ge", "word", "number").collect()
    assert(res2.length == 3)
    res2.foreach(a => {
      assert(a.getStartOffset == 9)
      assert(a.getEndOffset == 15)
      assert(a.getId == 3)
    })

    val res3 = annotatedSuccinctRDD.searchContaining("ge", "word", "three").collect()
    assert(res3.length == 1)
    assert(res3(0).getId == 5)
    assert(res3(0).getStartOffset == 16)
    assert(res3(0).getEndOffset == 21)
    assert(res3(0).getMetadata == "d^e")

    val res4 = annotatedSuccinctRDD.searchContaining("ge", "space", " ").collect()
    assert(res4.length == 6)
    res4.foreach(a => {
      assert(a.getId == 2 || a.getId == 4)
      assert(a.getStartOffset == 8 || a.getStartOffset == 15)
      assert(a.getEndOffset == 9 || a.getEndOffset == 16)
      assert(a.getMetadata == "")
    })

    val res5 = annotatedSuccinctRDD.searchContaining("ge", "word", "four").collect()
    assert(res5.length == 0)
  }

  test("Test regexContaining") {
    sc = new SparkContext(conf)

    val annotatedRDD = sc.parallelize(data)
    val annotatedSuccinctRDD = AnnotatedSuccinctRDD(annotatedRDD)

    // Check
    val res1 = annotatedSuccinctRDD.regexContaining("ge", "word", "one|two|three").collect()
    assert(res1.length == 3)
    res1.foreach(a => {
      assert(a.getStartOffset == 16)
      assert(a.getEndOffset == 19 | a.getEndOffset == 21)
      assert(a.getId == 5)
    })

    val res2 = annotatedSuccinctRDD.regexContaining("ge", "word", "four|five|six").collect()
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
