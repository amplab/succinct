package edu.berkeley.cs.succinct.sql

import edu.berkeley.cs.succinct.SuccinctIndexedFile
import org.apache.spark.sql.Row

/**
 * Iterator for a SuccinctTableRDD partition.
 *
 * @constructor Create a new SuccinctTableIterator with a SuccinctIndexedFile and a list of separators.
 * @param sBuf The underlying SuccinctIndexedFile.
 * @param succinctSerializer The serializer/deserializer for Succinct's representation of records.
 */
class SuccinctTableIterator private[succinct](sBuf: SuccinctIndexedFile, succinctSerializer: SuccinctSerDe)
  extends Iterator[Row] {

  var curRecordId: Int = 0

  /**
   * Returns true if there are more [[Row]]s to iterate over.
   *
   * @return true if there are more [[Row]]s to iterate over;
   *         false otherwise.
   */
  override def hasNext: Boolean = curRecordId < sBuf.getNumRecords

  /**
   * Returns the next [[Row]].
   *
   * @return The next [[Row]].
   */
  override def next(): Row = {
    val data = sBuf.getRecord(curRecordId)
    curRecordId += 1
    succinctSerializer.deserializeRow(data)
  }

}
