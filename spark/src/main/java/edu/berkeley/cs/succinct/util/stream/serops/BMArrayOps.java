package edu.berkeley.cs.succinct.util.stream.serops;

import edu.berkeley.cs.succinct.util.stream.LongArrayStream;

import java.io.IOException;

public class BMArrayOps {

  /**
   * Get value at specified index of a serialized bitmap array.
   *
   * @param B    Serialized bitmap array.
   * @param i    Index into serialized array.
   * @param bits Width in bits of value.
   * @return Value at specified index.
   */
  public static long getVal(LongArrayStream B, int i, int bits) throws IOException {
    assert (i >= 0);

    long val;
    long s = (long) (i) * bits;
    long e = s + (bits - 1);

    if ((s / 64) == (e / 64)) {
      val = B.get((int) (s / 64L)) << (s % 64);
      val = val >>> (63 - e % 64 + s % 64);
    } else {
      long val1 = B.get((int) (s / 64L)) << (s % 64);
      long val2 = B.get((int) (e / 64L)) >>> (63 - e % 64);
      val1 = val1 >>> (s % 64 - (e % 64 + 1));
      val = val1 | val2;
    }

    return val;
  }
}
