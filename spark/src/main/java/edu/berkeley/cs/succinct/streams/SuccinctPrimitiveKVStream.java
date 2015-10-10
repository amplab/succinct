package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctKV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;

public class SuccinctPrimitiveKVStream extends SuccinctIndexedFileStream
  implements SuccinctKV<Long> {

  private transient long[] keys;
  private transient BitSet invalidKeys;

  public SuccinctPrimitiveKVStream(Path filePath, Configuration conf) throws IOException {
    super(filePath, conf);
    FSDataInputStream is = getStream(filePath);
    is.seek(endOfIndexedFileStream);
    keys = readLongArrayFromStream(is);
    invalidKeys = BitSet.valueOf(readLongArrayFromStream(is));
    is.close();
  }

  public SuccinctPrimitiveKVStream(Path filePath) throws IOException {
    this(filePath, new Configuration());
  }

  /**
   * Helper function to write a long array to output stream.
   *
   * @param array The array to write.
   * @param os    The output stream to write data to.
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
   *
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
}
