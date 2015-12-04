package edu.berkeley.cs.succinct.util.stream.serops;

import edu.berkeley.cs.succinct.util.stream.RandomAccessLongStream;

import java.io.IOException;

public class BitMapOps {

  /**
   * Get bit at specified position in serialized bitmap.
   *
   * @param bitmap Serialized bitmap.
   * @param i      Index into serialized bitmap.
   * @return Value of bit.
   */
  public static long getBit(RandomAccessLongStream bitmap, int i) throws IOException {
    return ((bitmap.get((int) (bitmap.position() + (i / 64))) >>> (63L - i)) & 1L);
  }

  /**
   * Get value at specified index of serialized bitmap.
   *
   * @param bitmap Serialized bitmap.
   * @param pos    Position into bitmap.
   * @param bits   Width in bits of value.
   * @return Value at specified position.
   */
  public static long getValPos(RandomAccessLongStream bitmap, int pos, int bits)
    throws IOException {
    assert (pos >= 0);

    int basePos = (int) bitmap.position();
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
