package edu.berkeley.cs.succinct

/**
 * Iterator for a SuccinctRDD partition.
 *
 * @constructor Create a new SuccinctIterator from the underlying SuccinctBuffer.
 * @param sBuf The underlying SuccinctIndexedBuffer.
 */
class SuccinctIterator private[succinct](sBuf: SuccinctIndexedFile) extends Iterator[Array[Byte]] {

  var curRecordId: Int = 0

  /**
   * Returns true if there are more records to iterate over.
   *
   * @return true if there are more records to iterate over;
   *         false otherwise.
   */
  override def hasNext: Boolean = curRecordId < sBuf.getNumRecords

  /**
   * Returns the next tuple.
   *
   * @return The next tuple.
   */
  override def next(): Array[Byte] = {
    val data = sBuf.getRecord(curRecordId)
    curRecordId += 1
    data
  }
}
