package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctKV;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;

public class SuccinctPrimitiveKVBuffer extends SuccinctIndexedFileBuffer implements
  SuccinctKV<Long> {

  private transient long[] keys;
  private transient BitSet invalidKeys;

  public SuccinctPrimitiveKVBuffer(long[] keys, byte[] values, int[] offsets) {
    super(values, offsets);
    this.keys = keys;
    this.invalidKeys = new BitSet(keys.length);
  }

  public SuccinctPrimitiveKVBuffer(String path, StorageMode storageMode) {
    super(path, storageMode);
  }

  public SuccinctPrimitiveKVBuffer(DataInputStream is) {
    super(is);
  }

  @Override public Iterator<Long> keyIterator() {
    return new PrimitiveKeyIterator(keys);
  }

  @Override public byte[] get(Long key) {
    int i = Arrays.binarySearch(keys, key);
    return (i < 0 || invalidKeys.get(i)) ? null : getRecord(i);
  }

  @Override public boolean delete(Long key) {
    int i = Arrays.binarySearch(keys, key);
    if (i < 0) {
      return false;
    }
    invalidKeys.set(i);
    return true;
  }

  @Override public int numEntries() {
    return keys.length;
  }

  /**
   * Helper function to write a long array to output stream.
   *
   * @param array The array to write.
   * @param os The output stream to write data to.
   * @throws IOException
   */
  private void writeLongArrayToStream(long[] array, DataOutputStream os) throws IOException {
    os.writeInt(array.length);
    for (int i = 0; i < array.length; i++) {
      os.writeLong(array[i]);
    }
  }

  /**
   * Helper function to read a long array from input stream.
   * @param is The input stream to read from.
   * @return The output array.
   * @throws IOException
   */
  private long[] readLongArrayFromStream(DataInputStream is) throws IOException {
    int length = is.readInt();
    long[] array = new long[length];
    for (int i = 0; i < array.length; i++) {
      array[i] = is.readLong();
    }
    return array;
  }

  /**
   * Helper function to read a long array from input stream.
   * @param buf The input buffer to read from.
   * @return The output array.
   * @throws IOException
   */
  private long[] readLongArrayFromBuffer(ByteBuffer buf) {
    int length = buf.getInt();
    long[] array = new long[length];
    for (int i = 0; i < array.length; i++) {
      array[i] = buf.getLong();
    }
    return array;
  }

  /**
   * Write Succinct data structures to a DataOutputStream.
   *
   * @param os Output stream to write data to.
   * @throws IOException
   */
  @Override public void writeToStream(DataOutputStream os) throws IOException {
    super.writeToStream(os);
    writeLongArrayToStream(keys, os);
    writeLongArrayToStream(invalidKeys.toLongArray(), os);
  }

  /**
   * Reads Succinct data structures from a DataInputStream.
   *
   * @param is Stream to read data structures from.
   * @throws IOException
   */
  @Override public void readFromStream(DataInputStream is) throws IOException {
    super.readFromStream(is);
    keys = readLongArrayFromStream(is);
    invalidKeys = BitSet.valueOf(readLongArrayFromStream(is));
  }

  /**
   * Reads Succinct data structures from a ByteBuffer.
   *
   * @param buf ByteBuffer to read Succinct data structures from.
   */
  @Override public void mapFromBuffer(ByteBuffer buf) {
    super.mapFromBuffer(buf);
    keys = readLongArrayFromBuffer(buf);
    invalidKeys = BitSet.valueOf(readLongArrayFromBuffer(buf));
  }

  /**
   * Serialize SuccinctPrimitiveKVBuffer to OutputStream.
   *
   * @param oos ObjectOutputStream to write to.
   * @throws IOException
   */
  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.writeObject(keys);
    oos.writeObject(invalidKeys);
  }

  /**
   * Deserialize SuccinctPrimitiveKVBuffer from InputStream.
   *
   * @param ois ObjectInputStream to read from.
   * @throws IOException
   */
  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    keys = (long[]) ois.readObject();
    invalidKeys = (BitSet) ois.readObject();
  }
}
