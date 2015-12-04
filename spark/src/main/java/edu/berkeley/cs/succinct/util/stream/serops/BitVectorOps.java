package edu.berkeley.cs.succinct.util.stream.serops;

import edu.berkeley.cs.succinct.util.BitUtils;
import edu.berkeley.cs.succinct.util.stream.LongArrayStream;

import java.io.IOException;

public class BitVectorOps {

  /**
   * Get the bit at the specified index.
   *
   * @param data LongArrayStream representation of BitVector's data.
   * @param i The index into the BitVector.
   * @return The bit at the specified bit.
   */
  public static long getBit(LongArrayStream data, long i) throws IOException {
    // Skip one extra byte to account for size
    return BitUtils.getBit(data.get((int) (i >>> 6)), (int) (i % 64));
  }

  /**
   * Get the value at a particular offset into the BitVector.
   *
   * @param data LongArrayStream representation of BitVector's data.
   * @param pos The offset into the BitVector.
   * @param bits The bit-width of the value to get.
   * @return The value of specified bit-width at the specified offset into the BitVector.
   */
  public static long getValue(LongArrayStream data, long pos, int bits) throws IOException {
    if (bits == 0) {
      return 0;
    }

    int sOff = (int) (pos % 64);
    int sIdx = (int) (pos / 64);

    if (sOff + bits <= 64) {
      // Can be read from a single block
      return (data.get(sIdx) >>> sOff) & BitUtils.LOW_BITS_SET[bits];
    } else {
      // Must be read from two blocks
      return ((data.get(sIdx) >>> sOff) | (data.get(sIdx + 1) << (64 - sOff)))
        & BitUtils.LOW_BITS_SET[bits];
    }
  }

}
