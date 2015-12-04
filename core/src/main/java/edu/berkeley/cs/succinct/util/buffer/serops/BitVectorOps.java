package edu.berkeley.cs.succinct.util.buffer.serops;

import edu.berkeley.cs.succinct.util.BitUtils;

import java.nio.LongBuffer;

/**
 * Operations on Serialized BitVector. Supports read-only operations. </p>
 *
 * The buffer passed in as argument to all operations is the BitVector data (containing the 64-bit
 * blocks) represented as a LongBuffer.
 */
public class BitVectorOps {

  /**
   * Get the bit at the specified index.
   *
   * @param data LongBuffer representation of BitVector's data.
   * @param i The index into the BitVector.
   * @return The bit at the specified bit.
   */
  static long getBit(LongBuffer data, long i) {
    // Skip one extra byte to account for size
    return BitUtils.getBit(data.get((int) (i >>> 6)), (int) (i % 64));
  }

  /**
   * Get the value at a particular offset into the BitVector.
   *
   * @param data LongBuffer representation of BitVector's data.
   * @param pos The offset into the BitVector.
   * @param bits The bit-width of the value to get.
   * @return The value of specified bit-width at the specified offset into the BitVector.
   */
  static long getValue(LongBuffer data, long pos, int bits) {
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
