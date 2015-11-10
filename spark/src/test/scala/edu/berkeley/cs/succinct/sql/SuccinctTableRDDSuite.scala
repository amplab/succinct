package edu.berkeley.cs.succinct.sql

import com.google.common.io.Files
import edu.berkeley.cs.succinct.LocalSparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.scalatest.FunSuite

class SuccinctTableRDDSuite extends FunSuite with LocalSparkContext {

  val conf = new SparkConf().setAppName("test").setMaster("local")
    .set("spark.driver.allowMultipleContexts", "true")

  test("Test save and retrieve in memory") {
    sc = new SparkContext(conf)

    val baseRDD = sc.textFile(getClass.getResource("/table.dat").getFile)
      .map(_.split('|').toSeq)
    val firstRecord = baseRDD.first()
    val schema = StructType(firstRecord.map(StructField(_, StringType)))
    val tableRDD = baseRDD.filter(_ != firstRecord).map(Row.fromSeq(_))
    val succinctTableRDD = SuccinctTableRDD(tableRDD, schema).persist()

    val originalEntries = succinctTableRDD.collect()

    val tmpDir = Files.createTempDir()
    val succinctDir = tmpDir + "/succinct-table"
    succinctTableRDD.save(succinctDir)

    val newEntries = SuccinctTableRDD(sc, succinctDir, StorageLevel.MEMORY_ONLY).collect()

    assert(originalEntries === newEntries)
  }
}
