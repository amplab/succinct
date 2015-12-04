package edu.berkeley.cs.succinct.util.stream;

import edu.berkeley.cs.succinct.util.EliasGamma;
import edu.berkeley.cs.succinct.util.SuccinctConstants;
import edu.berkeley.cs.succinct.util.stream.serops.BitVectorOps;
import edu.berkeley.cs.succinct.util.stream.serops.IntVectorOps;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

public class DeltaEncodedIntStream {

  private FSDataInputStream stream;
  private long startPos;

  public DeltaEncodedIntStream(FSDataInputStream stream, long startPos) throws IOException {
    this.stream = stream;
    this.startPos = startPos;
  }

  /**
   * Compute the prefix-sum for the deltas starting at a specified offset (corresponding to a
   * particular sample) in the delta values for a specified number of delta values.
   *
   * @param deltas      The delta values represented as a LongArrayStream.
   * @param deltaOffset The offset into the delta BitVector.
   * @param untilIdx    The index until which the prefix sum should be computed.
   * @return The prefix-sum of delta values until the specified index.
   */
  private static int prefixSum(LongArrayStream deltas, int deltaOffset, int untilIdx)
    throws IOException {
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

  public int get(int i) throws IOException {
    stream.seek(startPos);

    // Read sampling rate
    int samplingRate = stream.readInt();

    // Read samples
    int sampleBits = stream.readInt();
    int sampleBlocks = stream.readInt();
    long endOfSamples = stream.getPos() + sampleBlocks * SuccinctConstants.LONG_SIZE_BYTES;

    LongArrayStream samples = new LongArrayStream(stream, stream.getPos(),
      sampleBlocks * SuccinctConstants.LONG_SIZE_BYTES);

    int samplesIdx = i / samplingRate;
    int deltaOffsetsIdx = i % samplingRate;
    int val = IntVectorOps.get(samples, samplesIdx, sampleBits);

    if (deltaOffsetsIdx == 0) {
      return val;
    }

    // Read deltaOffsets
    stream.seek(endOfSamples);
    int deltaOffsetBits = stream.readInt();
    int deltaOffsetBlocks = stream.readInt();
    long endOfDeltaOffsets =
      stream.getPos() + deltaOffsetBlocks * SuccinctConstants.LONG_SIZE_BYTES;
    LongArrayStream deltaOffsets = new LongArrayStream(stream, stream.getPos(),
      deltaOffsetBlocks * SuccinctConstants.LONG_SIZE_BYTES);
    int deltaOffset = IntVectorOps.get(deltaOffsets, samplesIdx, deltaOffsetBits);

    // Read deltas
    stream.seek(endOfDeltaOffsets);
    int deltaBlocks = stream.readInt();
    LongArrayStream deltas =
      new LongArrayStream(stream, stream.getPos(), deltaBlocks * SuccinctConstants.LONG_SIZE_BYTES);
    val += prefixSum(deltas, deltaOffset, deltaOffsetsIdx);

    return val;
  }
}
