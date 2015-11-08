package edu.berkeley.cs.succinct.json

import edu.berkeley.cs.succinct.LocalSparkContext
import edu.berkeley.cs.succinct.kv.SuccinctKVRDD
import org.apache.spark.SparkContext
import org.scalatest.FunSuite

import scala.util.Random

class SuccinctJsonRDDSuite extends FunSuite with LocalSparkContext {

  def genId(max: Int): Int = Math.abs(new Random().nextInt(max))

  test("Test get") {
    sc = new SparkContext("local", "test")

    val jsonRDD = sc.textFile(getClass.getResource("/json.dat").getFile)
    val kvRDD = jsonRDD.zipWithIndex().map(t => (t._2, t._1.getBytes))

    val succinctJsonRDD =
    val kvMap = kvRDD.collect().toMap

    val count = kvMap.size

    // Check
    (0 to 100).foreach(i => {
      val key = genKey(count)
      assert(kvMap(key) === succinctKVRDD.get(key))
    })
  }

}
