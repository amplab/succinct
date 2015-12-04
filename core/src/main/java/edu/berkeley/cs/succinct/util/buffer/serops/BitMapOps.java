package edu.berkeley.cs.succinct.util.buffer.serops;

import java.nio.LongBuffer;

public class BitMapOps {

  /**
   * Get bit at specified position in serialized bitmap.
   *
   * @param bitmap Serialized bitmap.
   * @param i      Index into serialized bitmap.
   * @return Value of bit.
   */
  public static long getBit(LongBuffer bitmap, int i) {
    return ((bitmap.get(bitmap.position() + (i / 64)) >>> (63L - i)) & 1L);
  }

  /**
   * Get value at specified index of serialized bitmap.
   *
   * @param bitmap Serialized bitmap.
   * @param pos    Position into bitmap.
   * @param bits   Width in bits of value.
   * @return Value at specified position.
   */
  public static long getValPos(LongBuffer bitmap, int pos, int bits) {
    assert (pos >= 0);

    int basePos = bitmap.position();
    long val;
    long s = (long) pos;
    long e = s + (bits - 1);

    if ((s / 64) == (e / 64)) {
      val = bitmap.get(basePos + (int) (s / 64L)) << (s % 64);
      val = val >>> (63 - e % 64 + s % 64);
    } else {
      val = bitmap.get(basePos + (int) (s / 64L)) << (s % 64);
      val = val >>> (s % 64 - (e % 64 + 1));
      val = val | (bitmap.get(basePos + (int) (e / 64L)) >>> (63 - e % 64));
    }
    assert (val >= 0);
    return val;
  }
}
