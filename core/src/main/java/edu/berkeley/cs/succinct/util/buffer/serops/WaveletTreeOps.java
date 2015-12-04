package edu.berkeley.cs.succinct.util.buffer.serops;

import java.nio.ByteBuffer;

public class WaveletTreeOps {

  /**
   * Get value encoded in wavelet tree.
   *
   * @param wTree      Serialized WaveletTree.
   * @param contextPos Context Position.
   * @param cellPos    Cell Position.
   * @param startIdx   Starting context index.
   * @param endIdx     Ending context index.
   * @return Decoded value.
   */
  public static long getValue(ByteBuffer wTree, int contextPos, int cellPos, int startIdx,
    int endIdx) {
    char m = (char) wTree.get();
    int left = (int) wTree.getLong();
    int right = (int) wTree.getLong();
    int dictPos = wTree.position();
    long p, v;

    // System.out.println("m = " + (int)m + " left = " + left + " right = " + right + " dictPos = " + dictPos);

    if (contextPos > m && contextPos <= endIdx) {
      if (right == 0) {
        return DictionaryOps.getSelect1(wTree, dictPos, cellPos);
      }
      p = getValue((ByteBuffer) wTree.position(right), contextPos, cellPos, m + 1, endIdx);
      v = DictionaryOps.getSelect1(wTree, dictPos, (int) p);
    } else {
      if (left == 0) {
        return DictionaryOps.getSelect0(wTree, dictPos, cellPos);
      }
      p = getValue((ByteBuffer) wTree.position(left), contextPos, cellPos, startIdx, m);
      v = DictionaryOps.getSelect0(wTree, dictPos, (int) p);
    }

    return v;
  }
}
