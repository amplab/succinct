package edu.berkeley.cs.succinct.storage

import edu.berkeley.cs.succinct.SuccinctIndexedFile
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer
import tachyon.client.TachyonFS

private[succinct] object TachyonStorageManager {

  def loadFromTachyon(path: String): SuccinctIndexedFile = {
    // val pathURI = new TachyonURI(path)
    val client = TachyonFS.get(path)
    val file = client.getFile(path)
    var buf = file.readByteBuffer(0)
    if (buf == null) {
      file.recache()
      buf = file.readByteBuffer(0)
    }
    buf.mData.reset()
    new SuccinctIndexedFileBuffer(buf.mData)
  }

}
