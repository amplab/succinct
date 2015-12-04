package edu.berkeley.cs.succinct.util.buffer.serops;

import java.nio.LongBuffer;

/**
 * Operations on Serialized IntVector. Supports read-only operations. </p>
 *
 * The buffer passed in as argument to all operations is the IntVector data (containing the 64-bit
 * blocks) represented as a LongBuffer.
 */
public class IntVectorOps {

  static public int get(LongBuffer data, int index, int bitWidth) {
    return (int) BitVectorOps.getValue(data, index * bitWidth, bitWidth);
  }
}
