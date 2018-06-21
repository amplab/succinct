package edu.berkeley.cs.succinct.util.buffer.serops;

import edu.berkeley.cs.succinct.util.EliasGamma;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * Operations on Serialized DeltaEncodedIntVector. Supports read-only operations. </p>
 * <p/>
 * The buffer passed in as argument to all operations is the ByteBuffer representation of the
 * DeltaEncodedIntVector.
 */
public class DeltaEncodedIntVectorOps {

  /**
   * Compute the prefix-sum for the deltas starting at a specified offset (corresponding to a
   * particular sample) in the delta values for a specified number of delta values.
   *
   * @param deltas      The delta values represented as a LongBuffer.
   * @param deltaOffset The offset into the delta BitVector.
   * @param untilIdx    The index until which the prefix sum should be computed.
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
        deltaSum +=
          BitVectorOps.getValue(deltas, currentDeltaOffset, deltaWidth) + (1L << deltaWidth);
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
          deltaSum +=
            BitVectorOps.getValue(deltas, currentDeltaOffset, deltaWidth) + (1L << deltaWidth);
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
   * @param i      Index into the DeltaEncodedIntVector.
   * @return Value at the specified index.
   */
  public static int get(ByteBuffer vector, int i) {
    // Read sampling rate
    int vectorPos = vector.position();
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
      vector.position(vectorPos);
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

    vector.position(vectorPos);

    return val;
  }

  /**
   * Binary search for a particular value within the samples of the DeltaEncodedVector.
   *
   * @param samples    The samples for the DeltaEncodedVector.
   * @param sampleBits The bit-width of each entry.
   * @param val        The value to search for.
   * @param s          The start offset for the search.
   * @param e          The end offset for the search.
   * @return The position in the samples array for the searched value.
   */
  private static int binarySearchSamples(LongBuffer samples, int sampleBits, int val, int s,
    int e) {
    int sp = s;
    int ep = e;
    int m, sVal;

    while (sp <= ep) {
      m = (sp + ep) / 2;

      sVal = IntVectorOps.get(samples, m, sampleBits);
      if (sVal == val) {
        ep = m;
        break;
      } else if (val < sVal)
        ep = m - 1;
      else
        sp = m + 1;
    }
    ep = Math.max(ep, 0);

    return ep;
  }

  /**
   * Binary search for a value within the DeltaEncodedVector.
   *
   * @param vector   The DeltaEncodedVector.
   * @param val      The value to search for.
   * @param startIdx The start index for the search.
   * @param endIdx   The end index for the search.
   * @param flag     Whether to get lower bound or upper bound; true - lower bound; false - upper bound.
   * @return The position in the DeltaEncodedVector for the searched value.
   */
  public static int binarySearch(ByteBuffer vector, int val, int startIdx, int endIdx,
    boolean flag) {
    if (endIdx < startIdx)
      return endIdx;

    // Save initial state
    int vectorPos = vector.position();

    // Read sampling rate
    int samplingRate = vector.getInt();

    // Read samples
    int sampleBits = vector.getInt();
    int sampleBlocks = vector.getInt();
    LongBuffer samples = (LongBuffer) vector.slice().asLongBuffer().limit(sampleBlocks);
    vector.position(vector.position() + sampleBlocks * 8);

    // Binary search within samples to get the nearest smaller sample
    int sampleOffset =
      binarySearchSamples(samples, sampleBits, val, startIdx / samplingRate, endIdx / samplingRate);

    // Set the limit on the number of delta values to decode
    int deltaLimit = Math.min(endIdx - (sampleOffset * samplingRate), samplingRate);

    // Read deltaOffsets
    int deltaOffsetBits = vector.getInt();
    int deltaOffsetBlocks = vector.getInt();
    LongBuffer deltaOffsets = (LongBuffer) vector.slice().asLongBuffer().limit(deltaOffsetBlocks);
    vector.position(vector.position() + deltaOffsetBlocks * 8);

    // Get offset into delta bitmap where decoding should start
    int currentDeltaOffset = IntVectorOps.get(deltaOffsets, sampleOffset, deltaOffsetBits);

    // Adjust the value being searched for
    val -= IntVectorOps.get(samples, sampleOffset, sampleBits);
    // Initialize the delta index and sum accumulated
    int deltaIdx = 0, deltaSum = 0;

    // Read deltas
    int deltaBlocks = vector.getInt();
    LongBuffer deltas = (LongBuffer) vector.slice().asLongBuffer().limit(deltaBlocks);


    // Keep decoding delta values until either:
    // (a) the accumulated sum exceeds the value itself, or,
    // (b) the delta index exceeds the limit on number of delta values to decode
    while (deltaSum < val && deltaIdx < deltaLimit) {
      int block = (int) BitVectorOps.getValue(deltas, currentDeltaOffset, 16);
      int cnt = EliasGamma.PrefixSum.count(block);
      int block_sum = EliasGamma.PrefixSum.sum(block);

      if (cnt == 0) {
        // If the prefixsum table for the block returns count == 0
        // this must mean the value spans more than 16 bits
        // read this manually
        int deltaWidth = 0;
        while (BitVectorOps.getBit(deltas, currentDeltaOffset) != 1) {
          deltaWidth++;
          currentDeltaOffset++;
        }
        currentDeltaOffset++;
        int decodedValue =
          (int) (BitVectorOps.getValue(deltas, currentDeltaOffset, deltaWidth) + (1L
            << deltaWidth));
        deltaSum += decodedValue;
        currentDeltaOffset += deltaWidth;
        deltaIdx += 1;
        // Roll back
        if (deltaIdx == samplingRate) {
          deltaIdx--;
          deltaSum -= decodedValue;
          break;
        }
      } else if (deltaSum + block_sum < val && deltaIdx + cnt < deltaLimit) {
        // If sum can be computed from the prefixsum table
        deltaSum += block_sum;
        currentDeltaOffset += EliasGamma.PrefixSum.offset(block);
        deltaIdx += cnt;
      } else {
        // Last few values, decode them without looking up table
        int lastDecodedValue = 0;
        while (deltaSum < val && deltaIdx < deltaLimit) {
          int deltaWidth = 0;
          while (BitVectorOps.getBit(deltas, currentDeltaOffset) != 1) {
            deltaWidth++;
            currentDeltaOffset++;
          }
          currentDeltaOffset++;
          lastDecodedValue =
            (int) (BitVectorOps.getValue(deltas, currentDeltaOffset, deltaWidth) + (1L
              << deltaWidth));
          deltaSum += lastDecodedValue;
          currentDeltaOffset += deltaWidth;
          deltaIdx += 1;
        }

        // Roll back
        if (deltaIdx == samplingRate) {
          deltaIdx--;
          deltaSum -= lastDecodedValue;
          break;
        }
      }
    }

    vector.position(vectorPos);

    // Obtain the required index for the binary search
    int res = sampleOffset * samplingRate + deltaIdx;

    // If it is an exact match, return the value
    if (val == deltaSum)
      return res;

    // Adjust the index based on whether we wanted lower bound or upper bound
    if (flag) {
      return (deltaSum < val) ? res : res - 1;
    } else {
      return (deltaSum > val) ? res : res + 1;
    }
  }
}
