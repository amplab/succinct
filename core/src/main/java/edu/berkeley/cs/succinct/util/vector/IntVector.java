package edu.berkeley.cs.succinct.util.vector;

import edu.berkeley.cs.succinct.util.BitUtils;
import edu.berkeley.cs.succinct.util.container.IntArrayList;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.IllegalFormatPrecisionException;

/**
 * Stores a vector of N integers, each with a maximum bit width of B compactly using N * B bits.
 * Supports efficiently getting and setting values.
 */
public class IntVector extends BitVector {

  private int bitWidth;

  /**
   * Default constructor for IntVector.
   */
  public IntVector() {
    super();
    this.bitWidth = 0;
  }

  /**
   * Initialize the IntVector with a specified capacity and element bit-width.
   *
   * @param numElements Number of elements in IntVector.
   * @param bitWidth    Width in bits of each element.
   */
  public IntVector(int numElements, int bitWidth) {
    super(numElements * bitWidth);
    this.bitWidth = bitWidth;
  }

  /**
   * Initialize the IntVector from underlying BitVector.
   *
   * @param bitWidth  Width in bits of each element.
   * @param bitVector The underlying bitVector.
   */
  public IntVector(int bitWidth, BitVector bitVector) {
    super(bitVector.getData());
    this.bitWidth = bitWidth;
  }

  /**
   * Initialize the IntVector with an existing integer array and a specified element bit-width.
   *
   * @param data     Array of integers to store.
   * @param bitWidth Width in bits of each element.
   */
  public IntVector(int[] data, int bitWidth) {
    super(data.length * bitWidth);
    this.bitWidth = bitWidth;
    for (int i = 0; i < data.length; i++) {
      if (BitUtils.bitWidth(data[i]) > bitWidth) {
        throw new IllegalFormatPrecisionException(bitWidth);
      }
      add(i, data[i]);
    }
  }

  /**
   * Initialize the IntVector with an existing integer array list and a specified element bit-width.
   *
   * @param data     Array list of integers to store.
   * @param bitWidth Width in bits of each element.
   */
  public IntVector(IntArrayList data, int bitWidth) {
    super(data.size() * bitWidth);
    this.bitWidth = bitWidth;
    for (int i = 0; i < data.size(); i++) {
      if (BitUtils.bitWidth(data.get(i)) > bitWidth) {
        throw new IllegalFormatPrecisionException(bitWidth);
      }
      add(i, data.get(i));
    }
  }

  /**
   * De-serialize the IntVector from a ByteBuffer.
   *
   * @param buf Input ByteBuffer.
   * @return The IntVector.
   */
  public static IntVector readFromBuffer(ByteBuffer buf) {
    int bitWidth = buf.getInt();

    if (bitWidth == 0) {
      return null;
    }

    return new IntVector(bitWidth, BitVector.readFromBuffer(buf));
  }

  /**
   * De-serialize the IntVector from a DataInputStream.
   *
   * @param in Input Stream.
   * @return The IntVector.
   * @throws IOException
   */
  public static IntVector readFromStream(DataInputStream in) throws IOException {
    int bitWidth = in.readInt();

    if (bitWidth == 0) {
      return null;
    }

    return new IntVector(bitWidth, BitVector.readFromStream(in));
  }

  /**
   * Get the width in bits of each element.
   *
   * @return Width in bits of each element.
   */
  public int getBitWidth() {
    return bitWidth;
  }

  /**
   * Add an element at a specified index into the IntVector.
   *
   * @param index   Index into the IntVector.
   * @param element Element to be added.
   */
  public void add(int index, int element) {
    setValue(index * bitWidth, element, bitWidth);
  }

  /**
   * Get the element at a specified index into the BitVector.
   *
   * @param index Index into the IntVector.
   * @return Element at specified index.
   */
  public int get(int index) {
    return (int) getValue(index * bitWidth, bitWidth);
  }

  /**
   * Get the size in bytes when serialized.
   *
   * @return Size in bytes when serialized.
   */
  public int serializedSize() {
    return 4 + super.serializedSize();
  }

  /**
   * Serialize the IntVector to a ByteBuffer.
   *
   * @param buf Output ByteBuffer.
   */
  public void writeToBuffer(ByteBuffer buf) {
    buf.putInt(bitWidth);
    super.writeToBuffer(buf);
  }

  /**
   * Serialize the IntVector to a DataOutputStream.
   *
   * @param out Output Stream.
   * @throws IOException
   */
  public void writeToStream(DataOutputStream out) throws IOException {
    out.writeInt(bitWidth);
    super.writeToStream(out);
  }

}
