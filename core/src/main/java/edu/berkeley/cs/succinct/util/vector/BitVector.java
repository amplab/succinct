package edu.berkeley.cs.succinct.util.vector;

import edu.berkeley.cs.succinct.util.BitUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A compactly represented vector of bits. Internally holds the data as an array of 64 bit integers
 * and supports efficiently setting, getting and clearing bits in the bit vector.
 */
public class BitVector {

  private long[] data;

  /**
   * Default constructor for BitVector.
   */
  public BitVector() {
    this.data = null;
  }

  /**
   * Constructor to initialize the BitVector. The size of the BitVector snaps to the smallest number
   * of 64-bit blocks that can accommodate the specified number of bits.
   *
   * @param sizeInBits The requested size in bits of the BitVector; note that the actual size of the
   *                   BitVector may be larger than this size, but no larger than 63 bits.
   */
  public BitVector(long sizeInBits) {
    this.data = new long[BitUtils.bitsToBlocks64(sizeInBits)];
  }

  /**
   * Constructor to initialize the BitVector from existing underlying data.
   *
   * @param data The underlying data for the BitVector.
   */
  public BitVector(long[] data) {
    this.data = data;
  }

  /**
   * Get the underlying blocks in the BitVector.
   *
   * @return The underlying blocks in the BitVector.
   */
  public long[] getData() {
    return data;
  }

  /**
   * Get the number of 64-bit blocks in the bit-vector. The number of bits the bit vector can hold is
   * 64 x numBlocks.
   *
   * @return The number of 64-bit blocks in the bit-vector.
   */
  public int numBlocks() {
    return data.length;
  }

  /**
   * Set the bit at the specified index.
   *
   * @param i The index into the BitVector.
   */
  public void setBit(long i) {
    data[((int) (i >>> 6))] = BitUtils.setBit(data[((int) (i >>> 6))], (int) (i % 64));
  }

  /**
   * Clear the bit at the specified index.
   *
   * @param i The index into the BitVector.
   */
  public void clearBit(long i) {
    data[((int) (i >>> 6))] = BitUtils.clearBit(data[((int) (i >>> 6))], (int) (i % 64));
  }

  /**
   * Get the bit at the specified index.
   *
   * @param i The index into the BitVector.
   * @return The bit at the specified bit.
   */
  public long getBit(long i) {
    return BitUtils.getBit(data[((int) (i >>> 6))], (int) (i % 64));
  }

  /**
   * Set the value at a particular offset into the BitVector.
   *
   * @param pos The offset into the bit vector.
   * @param value The value to set.
   * @param bits The bit-width of the value to set.
   */
  public void setValue(long pos, long value, int bits) {
    if (bits == 0) {
      return;
    }

    int sOff = (int) (pos % 64);
    int sIdx = (int) (pos / 64);

    if (sOff + bits <= 64) {
      // Can be accommodated in 1 bitmap block
      data[sIdx] = (data[sIdx]
        & (BitUtils.LOW_BITS_SET[sOff] | BitUtils.LOW_BITS_UNSET[sOff + bits])) | value << sOff;
    } else {
      // Must use 2 bitmap blocks
      data[sIdx] = (data[sIdx] & BitUtils.LOW_BITS_SET[sOff]) | value << sOff;
      data[sIdx + 1] = (data[sIdx + 1] & BitUtils.LOW_BITS_UNSET[(sOff + bits) % 64])
        | (value >>> (64 - sOff));
    }
  }

  /**
   * Get the value at a particular offset into the BitVector.
   *
   * @param pos The offset into the BitVector.
   * @param bits The bit-width of the value to get.
   * @return The value of specified bit-width at the specified offset into the BitVector.
   */
  public long getValue(long pos, int bits) {
    if (bits == 0) {
      return 0;
    }

    int sOff = (int) (pos % 64);
    int sIdx = (int) (pos / 64);

    if (sOff + bits <= 64) {
      // Can be read from a single block
      return (data[sIdx] >>> sOff) & BitUtils.LOW_BITS_SET[bits];
    } else {
      // Must be read from two blocks
      return ((data[sIdx] >>> sOff) | (data[sIdx + 1] << (64 - sOff)))
        & BitUtils.LOW_BITS_SET[bits];
    }
  }

  /**
   * Get the size in bytes when serialized.
   *
   * @return Size in bytes when serialized.
   */
  public int serializedSize() {
    return 8 * data.length + 4;
  }

  /**
   * Serialize the BitVector to a ByteBuffer.
   *
   * @param buf Output ByteBuffer.
   */
  public void writeToBuffer(ByteBuffer buf) {
    buf.putInt(data.length);
    for (int i = 0; i < data.length; i++) {
      buf.putLong(data[i]);
    }
  }

  /**
   * Serialize the BitVector to a DataOutputStream.
   *
   * @param out Output Stream.
   * @throws IOException
   */
  public void writeToStream(DataOutputStream out) throws IOException {
    out.writeInt(data.length);
    for (int i = 0; i < data.length; i++) {
      out.writeLong(data[i]);
    }
  }

  /**
   * De-serialize the BitVector from a ByteBuffer.
   *
   * @param buf Input ByteBuffer.
   * @return The BitVector.
   */
  public static BitVector readFromBuffer(ByteBuffer buf) {
    int numBlocks = buf.getInt();

    if (numBlocks == 0) {
      return null;
    }

    long[] data = new long[numBlocks];
    for (int i = 0; i < data.length; i++) {
      data[i] = buf.getLong();
    }
    return new BitVector(data);
  }

  /**
   * De-serialize the BitVector from a DataInputStream.
   *
   * @param in Input Stream.
   * @return The BitVector.
   * @throws IOException
   */
  public static BitVector readFromStream(DataInputStream in) throws IOException {
    int numBlocks = in.readInt();

    if (numBlocks == 0) {
      return null;
    }

    long[] data = new long[numBlocks];
    for (int i = 0; i < data.length; i++) {
      data[i] = in.readLong();
    }
    return new BitVector(data);
  }

}
