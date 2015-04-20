package edu.berkeley.cs.succinct.sql

import edu.berkeley.cs.succinct.{SuccinctBuffer, SuccinctIndexedBuffer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * Iterator for a SuccinctTableRDD partition.
 *
 * @constructor Create a new SuccinctTableIterator with a SuccinctBuffer and a list of separators.
 * @param sBuf The underlying SuccinctBuffer.
 * @param separators The list of unique separators for each attribute.
 */
class SuccinctTableIterator private[succinct](sBuf: SuccinctBuffer, separators: Array[Byte], schema: StructType)
  extends Iterator[Row] {

  var curPos: Int = 0

  /**
   * Returns true if there are more [[Row]]s to iterate over.
   *
   * @return true if there are more [[Row]]s to iterate over;
   *         false otherwise.
   */
  override def hasNext: Boolean = (curPos < sBuf.getOriginalSize() - 1)

  /**
   * Returns the next [[Row]].
   *
   * @return The next [[Row]].
   */
  override def next(): Row = {
    val data = sBuf.extractUntil(curPos, SuccinctIndexedBuffer.getRecordDelim)
    curPos = curPos + data.length + 1
    SuccinctSerializer.deserializeRow(data, separators, schema)
  }

}
