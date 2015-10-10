package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctKV;

import java.io.*;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

public class SuccinctKVBuffer<K extends Comparable<? super K>> extends SuccinctIndexedFileBuffer
  implements SuccinctKV<K> {

  transient private K[] keys;
  transient private BitSet invalidKeys;

  /**
   * Constructor to initialize SuccinctKVBuffer from keys, flattened values and value offsets.
   * Note: Keys must be sorted.
   *
   * @param keys    Array of input keys (sorted).
   * @param values  Buffer of flattened values.
   * @param offsets Array of offsets into flattened values corresponding to keys.
   */
  public SuccinctKVBuffer(K[] keys, byte[] values, int[] offsets) {
    super(values, offsets);
    this.keys = keys;
    this.invalidKeys = new BitSet(keys.length);
  }

  public SuccinctKVBuffer(List<K> keys, byte[] values, int[] offsets) {
    super(values, offsets);
    this.keys = (K[]) keys.toArray(new Comparable<?>[keys.size()]);
    this.invalidKeys = new BitSet(keys.size());
  }

  public SuccinctKVBuffer(String path, StorageMode storageMode) {
    super(path, storageMode);
  }

  public SuccinctKVBuffer(DataInputStream is) {
    super(is);
  }

  @Override public Iterator<K> keyIterator() {
    return new KeyIterator<K>(keys);
  }

  @Override public byte[] get(K key) {
    int i = Arrays.binarySearch(keys, key);
    return (i < 0 || invalidKeys.get(i)) ? null : getRecord(i);
  }

  @Override public boolean delete(K key) {
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
   * Write Succinct data structures to a DataOutputStream.
   *
   * @param os Output stream to write data to.
   * @throws IOException
   */
  @Override public void writeToStream(DataOutputStream os) throws IOException {
    super.writeToStream(os);
    ObjectOutputStream oos = new ObjectOutputStream(os);
    oos.writeObject(keys);
    oos.writeObject(invalidKeys);
  }

  /**
   * Reads Succinct data structures from a DataInputStream.
   *
   * @param is Stream to read data structures from.
   * @throws IOException
   */
  @Override public void readFromStream(DataInputStream is) throws IOException {
    super.readFromStream(is);
    ObjectInputStream ois = new ObjectInputStream(is);
    try {
      keys = (K[]) ois.readObject();
      invalidKeys = (BitSet) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Serialize SuccinctKVBuffer to OutputStream.
   *
   * @param oos ObjectOutputStream to write to.
   * @throws IOException
   */
  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.writeObject(keys);
    oos.writeObject(invalidKeys);
  }

  /**
   * Deserialize SuccinctKVBuffer from InputStream.
   *
   * @param ois ObjectInputStream to read from.
   * @throws IOException
   */
  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    keys = (K[]) ois.readObject();
    invalidKeys = (BitSet) ois.readObject();
  }
}
