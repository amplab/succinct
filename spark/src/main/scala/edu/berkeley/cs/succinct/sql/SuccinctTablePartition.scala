package org.apache.spark.succinct.sql

import java.io.DataOutputStream

import edu.berkeley.cs.succinct.SuccinctTable
import edu.berkeley.cs.succinct.SuccinctTable.QueryType
import edu.berkeley.cs.succinct.buffers.SuccinctTableBuffer
import edu.berkeley.cs.succinct.sql.SuccinctSerDe
import edu.berkeley.cs.succinct.streams.SuccinctTableStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{KnownSizeEstimation, SizeEstimator}

class SuccinctTablePartition(
                              succinctIndexedFile: SuccinctTable,
                              succinctSerDe: SuccinctSerDe
                            ) extends KnownSizeEstimation {

  override val estimatedSize: Long = {
    succinctIndexedFile.getCompressedSize + SizeEstimator.estimate(succinctSerDe)
  }

  def iterator: Iterator[Row] = {
    new Iterator[Row] {
      var curRecordId: Int = 0

      override def hasNext: Boolean = curRecordId < succinctIndexedFile.getNumRecords

      override def next(): Row = {
        val data = succinctIndexedFile.getRecordBytes(curRecordId)
        curRecordId += 1
        succinctSerDe.deserializeRow(data)
      }
    }
  }

  def prune(reqColumnsCheck: Map[String, Boolean]): Iterator[Row] = {
    new Iterator[Row] {
      var curRecordId: Int = 0

      override def hasNext: Boolean = curRecordId < succinctIndexedFile.getNumRecords

      override def next(): Row = {
        val data = succinctIndexedFile.getRecordBytes(curRecordId)
        curRecordId += 1
        succinctSerDe.deserializeRow(data, reqColumnsCheck)
      }
    }
  }

  def pruneAndFilter(
                      reqColumnsCheck: Map[String, Boolean],
                      queryTypes: Array[QueryType],
                      queries: Array[Array[Array[Byte]]]): Iterator[Row] = {
    new Iterator[Row] {
      val searchResults = succinctIndexedFile.recordMultiSearchIds(queryTypes, queries).iterator

      override def hasNext: Boolean = searchResults.hasNext

      override def next(): Row = {
        val data = succinctIndexedFile.getRecordBytes(searchResults.next())
        succinctSerDe.deserializeRow(data, reqColumnsCheck)
      }
    }
  }

  def count: Long = succinctIndexedFile.getNumRecords

  def writeToStream(dataOutputStream: DataOutputStream): Unit = {
    succinctIndexedFile.writeToStream(dataOutputStream)
  }
}

object SuccinctTablePartition {
  def apply(
             partitionLocation: String,
             succinctSerDe: SuccinctSerDe,
             storageLevel: StorageLevel): SuccinctTablePartition = {
    val path = new Path(partitionLocation)
    val fs = FileSystem.get(path.toUri, new Configuration())
    val is = fs.open(path)
    val succinctTableFile = storageLevel match {
      case StorageLevel.MEMORY_ONLY =>
        new SuccinctTableBuffer(is)
      case StorageLevel.DISK_ONLY =>
        new SuccinctTableStream(path)
      case _ =>
        new SuccinctTableBuffer(is)
    }
    is.close()
    new SuccinctTablePartition(succinctTableFile, succinctSerDe)
  }
}
