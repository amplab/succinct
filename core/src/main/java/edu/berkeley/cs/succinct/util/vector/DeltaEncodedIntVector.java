package edu.berkeley.cs.succinct.util.vector;

import edu.berkeley.cs.succinct.util.BitUtils;
import edu.berkeley.cs.succinct.util.EliasGamma;
import gnu.trove.list.array.TIntArrayList;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Stores a sorted array of integers as a delta encoded vector. The encoding scheme used is
 * Elias Gamma.
 */
public class DeltaEncodedIntVector {
  private IntVector samples;
  private IntVector deltaOffsets;
  private BitVector deltas;
  private int samplingRate;

  /**
   * Default constructor for DeltaEncodedIntVector.
   */
  public DeltaEncodedIntVector() {
    this.samplingRate = 0;
    this.samples = null;
    this.deltaOffsets = null;
    this.deltas = null;
  }

  /**
   * Creates a DeltaEncodedVector from a sorted array of integers with a specified sampling rate.
   *
   * @param elements     Sorted array of integers.
   * @param samplingRate Sampling rate for delta encoding.
   */
  public DeltaEncodedIntVector(int[] elements, int samplingRate) {
    this.samplingRate = samplingRate;
    encode(elements, 0, elements.length);
  }

  /**
   * Creates a DeltaEncodedVector from a sorted sequence of integers of specified length starting at
   * a specified offset an array, with a specified sampling rate.
   *
   * @param data         The larger array containing the sorted array of integers.
   * @param startOffset  The start offset into the larger array for the sorted sequence of integers.
   * @param length       The number of integers to consider in the larger array.
   * @param samplingRate Sampling rate for delta encoding.
   */
  public DeltaEncodedIntVector(int[] data, int startOffset, int length, int samplingRate) {
    this.samplingRate = samplingRate;
    encode(data, startOffset, length);
  }

  /**
   * Creates a DeltaEncodedVector from underlying data.
   *
   * @param samples      Samples for the DeltaEncodedVector.
   * @param deltaOffsets Delta offsets for the DeltaEncodedVector.
   * @param deltas       The delta values for the DeltaEncodedVector.
   * @param samplingRate Sampling rate for delta encoding.
   */
  public DeltaEncodedIntVector(IntVector samples, IntVector deltaOffsets, BitVector deltas,
    int samplingRate) {
    this.samples = samples;
    this.deltaOffsets = deltaOffsets;
    this.deltas = deltas;
    this.samplingRate = samplingRate;
  }

  /**
   * Get the samples for the DeltaEncodedVector.
   *
   * @return Samples for the DeltaEncodedVector.
   */
  public IntVector getSamples() {
    return samples;
  }

  /**
   * Get the offsets corresponding to delta value blocks.
   *
   * @return Offsets to delta value blocks.
   */
  public IntVector getDeltaOffsets() {
    return deltaOffsets;
  }

  /**
   * Get the Elias Gamma encoded delta values.
   *
   * @return Elias Gamma encoded delta values.
   */
  public BitVector getDeltas() {
    return deltas;
  }

  /**
   * Get the sampling rate for the DeltaEncodedIntVector.
   *
   * @return Sampling rate for the DeltaEncodedIntVector.
   */
  public int getSamplingRate() {
    return samplingRate;
  }

  /**
   * Encodes the array of integers with delta encoding.
   *
   * @param elements Sorted array of integers.
   */
  private void encode(int[] elements, int startOffset, int length) {
    if (length == 0) {
      return;
    }

    TIntArrayList samplesBuf, deltasBuf, deltaOffsetsBuf;
    samplesBuf = new TIntArrayList(0);
    deltasBuf = new TIntArrayList(0);
    deltaOffsetsBuf = new TIntArrayList(0);

    int maxSample = 0;
    int lastValue = 0;
    int totalDeltaCount = 0, deltaCount = 0;
    int cumulativeDeltaSize = 0;
    int maxOffset = 0;

    for (int i = 0; i < length; i++) {
      if (i % samplingRate == 0) {
        samplesBuf.add(elements[startOffset + i]);
        if (elements[startOffset + i] > maxSample) {
          maxSample = elements[startOffset + i];
        }
        if (cumulativeDeltaSize > maxOffset)
          maxOffset = cumulativeDeltaSize;
        deltaOffsetsBuf.add(cumulativeDeltaSize);
        if (i != 0) {
          assert (deltaCount == samplingRate - 1);
          totalDeltaCount += deltaCount;
          deltaCount = 0;
        }
      } else {
        if (elements[startOffset + i] <= lastValue) {
          throw new IllegalStateException(
            "Delta value cannot be 0 or negative: " + (elements[startOffset + i] - lastValue));
        }
        int delta = elements[startOffset + i] - lastValue;
        deltasBuf.add(delta);

        cumulativeDeltaSize += EliasGamma.encodingSize(delta);
        deltaCount++;
      }
      lastValue = elements[startOffset + i];
    }
    totalDeltaCount += deltaCount;

    assert totalDeltaCount == deltasBuf.size();
    assert samplesBuf.size() + deltasBuf.size() == length;
    assert deltaOffsetsBuf.size() == samplesBuf.size();

    int sampleBits = BitUtils.bitWidth(maxSample);
    int deltaOffsetBits = BitUtils.bitWidth(maxOffset);

    if (samplesBuf.size() == 0) {
      samples = null;
    } else {
      samples = new IntVector(samplesBuf.toArray(), sampleBits);
    }

    if (cumulativeDeltaSize == 0) {
      deltas = null;
    } else {
      deltas = new BitVector(cumulativeDeltaSize + 16); // One extra block for safety
      encodeDeltas(deltasBuf.toArray());
    }

    if (deltaOffsetsBuf.size() == 0) {
      deltaOffsets = null;
    } else {
      deltaOffsets = new IntVector(deltaOffsetsBuf.toArray(), deltaOffsetBits);
    }
  }

  /**
   * Encode the delta values using EliasGamma encoding.
   *
   * @param deltasArray Array of integers containing delta values.
   */
  private void encodeDeltas(int[] deltasArray) {
    long pos = 0;
    for (int i = 0; i < deltasArray.length; i++) {
      // System.out.println("i = " + i + " value = " + deltasArray[i]);
      int deltaBits = BitUtils.bitWidth(deltasArray[i]) - 1;
      pos += deltaBits;

      assert 1L << deltaBits <= deltasArray[i];

      deltas.setBit(pos++);
      deltas.setValue(pos, deltasArray[i] - (1L << deltaBits), deltaBits);
      pos += deltaBits;
    }
  }

  /**
   * Compute the prefix-sum for the deltas starting at a specified offset (corresponding to a
   * particular sample) in the delta values for a specified number of delta values.
   *
   * @param deltaOffset The offset into the delta BitVector.
   * @param untilIdx    The index until which the prefix sum should be computed.
   * @return The prefix-sum of delta values until the specified index.
   */
  private int prefixSum(int deltaOffset, int untilIdx) {
    int deltaSum = 0;
    long deltaIdx = 0;
    long currentDeltaOffset = deltaOffset;
    while (deltaIdx != untilIdx) {
      int block = (int) deltas.getValue(currentDeltaOffset, 16);
      int cnt = EliasGamma.PrefixSum.count(block);
      if (cnt == 0) {
        // If the prefixsum table for the block returns count = 0
        // this must mean the value spans more than 16 bits
        // read this manually
        int deltaWidth = 0;
        while (deltas.getBit(currentDeltaOffset) != 1) {
          deltaWidth++;
          currentDeltaOffset++;
        }
        currentDeltaOffset++;
        deltaSum += deltas.getValue(currentDeltaOffset, deltaWidth) + (1L << deltaWidth);
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
          while (deltas.getBit(currentDeltaOffset) != 1) {
            deltaWidth++;
            currentDeltaOffset++;
          }
          currentDeltaOffset++;
          deltaSum += deltas.getValue(currentDeltaOffset, deltaWidth) + (1L << deltaWidth);
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
   * @param i Index into the DeltaEncodedIntVector.
   * @return Value at the specified index.
   */
  public int get(int i) {
    // Get offsets
    int samplesIdx = i / samplingRate;
    int deltaOffsetsIdx = i % samplingRate;
    int val = samples.get(samplesIdx);

    if (deltaOffsetsIdx == 0)
      return val;

    int deltaOffset = deltaOffsets.get(samplesIdx);
    val += prefixSum(deltaOffset, deltaOffsetsIdx);
    return val;
  }

  /**
   * Get the size in bytes when serialized.
   *
   * @return Size in bytes when serialized.
   */
  public int serializedSize() {
    int samplesSize = (samples == null) ? 4 : samples.serializedSize();
    int deltaOffsetsSize = (deltaOffsets == null) ? 4 : deltaOffsets.serializedSize();
    int deltasSize = (deltas == null) ? 4 : deltas.serializedSize();
    return 4 + samplesSize + deltaOffsetsSize + deltasSize;
  }

  /**
   * Serialize the DeltaEncodedIntVector to a ByteBuffer.
   *
   * @param buf Output ByteBuffer.
   */
  public void writeToBuffer(ByteBuffer buf) {
    buf.putInt(samplingRate);
    if (samples != null) {
      samples.writeToBuffer(buf);
    } else {
      buf.putInt(0);
    }
    if (deltaOffsets != null) {
      deltaOffsets.writeToBuffer(buf);
    } else {
      buf.putInt(0);
    }
    if (deltas != null) {
      deltas.writeToBuffer(buf);
    } else {
      buf.putInt(0);
    }
  }

  /**
   * Serialize the DeltaEncodedIntVector to a DataOutputStream.
   *
   * @param out Output Stream.
   * @throws IOException
   */
  public void writeToStream(DataOutputStream out) throws IOException {
    out.writeInt(samplingRate);
    if (samples != null) {
      samples.writeToStream(out);
    } else {
      out.writeInt(0);
    }
    if (deltaOffsets != null) {
      deltaOffsets.writeToStream(out);
    } else {
      out.writeInt(0);
    }
    if (deltas != null) {
      deltas.writeToStream(out);
    } else {
      out.writeInt(0);
    }
  }


  /**
   * De-serialize the DeltaEncodedIntVector from a ByteBuffer.
   *
   * @param buf Input ByteBuffer.
   */
  public static DeltaEncodedIntVector readFromBuffer(ByteBuffer buf) {
    int samplingRate = buf.getInt();
    IntVector samples = IntVector.readFromBuffer(buf);
    IntVector deltaOffsets = IntVector.readFromBuffer(buf);
    BitVector deltas = BitVector.readFromBuffer(buf);
    if (samples == null) {
      return null;
    }
    return new DeltaEncodedIntVector(samples, deltaOffsets, deltas, samplingRate);
  }

  /**
   * De-serialize the DeltaEncodedIntVector from a DataInputStream.
   *
   * @param in Input Stream.
   * @throws IOException
   */
  public static DeltaEncodedIntVector readFromStream(DataInputStream in) throws IOException {
    int samplingRate = in.readInt();
    IntVector samples = IntVector.readFromStream(in);
    IntVector deltaOffsets = IntVector.readFromStream(in);
    BitVector deltas = BitVector.readFromStream(in);
    if (samples == null) {
      return null;
    }
    return new DeltaEncodedIntVector(samples, deltaOffsets, deltas, samplingRate);
  }
}
