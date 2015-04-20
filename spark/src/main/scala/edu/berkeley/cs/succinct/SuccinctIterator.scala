package edu.berkeley.cs.succinct

/**
 * Iterator for a SuccinctRDD partition.
 *
 * @constructor Create a new SuccinctIterator from the underlying SuccinctBuffer.
 * @param sBuf The underlying SuccinctBuffer.
 */
class SuccinctIterator private[succinct](sBuf: SuccinctBuffer) extends Iterator[Array[Byte]] {

  var curPos: Int = 0
  var buf: SuccinctBuffer = sBuf

  /**
   * Returns true if there are more records to iterate over.
   *
   * @return true if there are more records to iterate over;
   *         false otherwise.
   */
  override def hasNext: Boolean = (curPos < sBuf.getOriginalSize() - 1)

  /**
   * Returns the next tuple.
   *
   * @return The next tuple.
   */
  override def next(): Array[Byte] = {
    val data = buf.extractUntil(curPos, SuccinctIndexedBuffer.getRecordDelim)
    curPos = curPos + data.length + 1
    data
  }
}
