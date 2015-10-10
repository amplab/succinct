package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctKV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;

public class SuccinctKVStream<K extends Comparable<K>> extends SuccinctIndexedFileStream implements
  SuccinctKV<K> {

  transient private K[] keys;
  transient private BitSet invalidKeys;

  public SuccinctKVStream(Path filePath, Configuration conf)
    throws IOException, ClassNotFoundException {
    super(filePath, conf);
    FSDataInputStream is = getStream(filePath);
    is.seek(endOfIndexedFileStream);
    ObjectInputStream ois = new ObjectInputStream(is);
    keys = (K[]) ois.readObject();
    invalidKeys = (BitSet) ois.readObject();
    is.close();
  }

  public SuccinctKVStream(Path filePath) throws IOException, ClassNotFoundException {
    this(filePath, new Configuration());
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
}
