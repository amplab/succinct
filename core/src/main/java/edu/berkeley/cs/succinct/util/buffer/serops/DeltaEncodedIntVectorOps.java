package edu.berkeley.cs.succinct.util.buffer.serops;

import edu.berkeley.cs.succinct.util.EliasGamma;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * Operations on Serialized DeltaEncodedIntVector. Supports read-only operations. </p>
 *
 * The buffer passed in as argument to all operations is the ByteBuffer representation of the
 * DeltaEncodedIntVector.
 */
public class DeltaEncodedIntVectorOps {

  /**
   * Compute the prefix-sum for the deltas starting at a specified offset (corresponding to a
   * particular sample) in the delta values for a specified number of delta values.
   *
   * @param deltas The delta values represented as a LongBuffer.
   * @param deltaOffset The offset into the delta BitVector.
   * @param untilIdx The index until which the prefix sum should be computed.
   * @return The prefix-sum of delta values until the specified index.
   */
  private static int prefixSum(LongBuffer deltas, int deltaOffset, int untilIdx) {
    int deltaSum = 0;
    long deltaIdx = 0;
    long currentDeltaOffset = deltaOffset;
    while (deltaIdx != untilIdx) {
      int block = (int) BitVectorOps.getValue(deltas, currentDeltaOffset, 16);
      int cnt = EliasGamma.PrefixSum.count(block);
      if (cnt == 0) {
        // If the prefixsum table for the block returns count = 0
        // this must mean the value spans more than 16 bits
        // read this manually
        int deltaWidth = 0;
        while (BitVectorOps.getBit(deltas, currentDeltaOffset) != 1) {
          deltaWidth++;
          currentDeltaOffset++;
        }
        currentDeltaOffset++;
        deltaSum += BitVectorOps.getValue(deltas, currentDeltaOffset, deltaWidth) + (1L << deltaWidth);
        currentDeltaOffset += deltaWidth;
        deltaIdx += 1;
      } else if (deltaIdx + cnt <= untilIdx) {
        // If sum can be computed from the prefixsum table
        deltaSum += EliasGamma.PrefixSum.sum(block);
        currentDeltaOffset += EliasGamma.PrefixSum.offset(block);
        deltaIdx += cnt;
      } else {
        // Last few values, decode them without looking up table
        while (deltaIdx != untilIdx) {
          int deltaWidth = 0;
          while (BitVectorOps.getBit(deltas, currentDeltaOffset) != 1) {
            deltaWidth++;
            currentDeltaOffset++;
          }
          currentDeltaOffset++;
          deltaSum += BitVectorOps.getValue(deltas, currentDeltaOffset, deltaWidth) + (1L << deltaWidth);
          currentDeltaOffset += deltaWidth;
          deltaIdx += 1;
        }
      }
    }
    return deltaSum;
  }

  /**
   * Get the value at a specified index into the DeltaEncodedIntVector.
   *
   * @param vector DeltaEncodedIntVector represented as ByteBuffer.
   * @param i Index into the DeltaEncodedIntVector.
   * @return Value at the specified index.
   */
  public static int get(ByteBuffer vector, int i) {
    // Read sampling rate
    int samplingRate = vector.getInt();

    // Read samples
    int sampleBits = vector.getInt();
    int sampleBlocks = vector.getInt();
    LongBuffer samples = (LongBuffer) vector.slice().asLongBuffer().limit(sampleBlocks);
    vector.position(vector.position() + sampleBlocks * 8);

    int samplesIdx = i / samplingRate;
    int deltaOffsetsIdx = i % samplingRate;
    int val = IntVectorOps.get(samples, samplesIdx, sampleBits);

    if (deltaOffsetsIdx == 0) {
      vector.rewind();
      return val;
    }

    // Read deltaOffsets
    int deltaOffsetBits = vector.getInt();
    int deltaOffsetBlocks = vector.getInt();
    LongBuffer deltaOffsets = (LongBuffer) vector.slice().asLongBuffer().limit(deltaOffsetBlocks);
    vector.position(vector.position() + deltaOffsetBlocks * 8);

    // Read deltas
    int deltaBlocks = vector.getInt();
    LongBuffer deltas = (LongBuffer) vector.slice().asLongBuffer().limit(deltaBlocks);

    int deltaOffset = IntVectorOps.get(deltaOffsets, samplesIdx, deltaOffsetBits);
    val += prefixSum(deltas, deltaOffset, deltaOffsetsIdx);

    vector.rewind();

    return val;
  }
}
