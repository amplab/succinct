package edu.berkeley.cs.succinct;

import java.util.Iterator;

/**
 * Key-Value interface for Succinct. Values are byte arrays, while key type is specified as parameter.
 *
 * @param <K> The type for the key.
 */
public interface SuccinctKV<K> extends SuccinctIndexedFile {

  class KeyIterator<K> implements Iterator<K> {

    private int curKeyIdx;
    private K[] keys;

    public KeyIterator(K[] keys) {
      curKeyIdx = 0;
      this.keys = keys;
    }

    @Override public boolean hasNext() {
      return (curKeyIdx < keys.length);
    }

    @Override public K next() {
      return keys[curKeyIdx++];
    }

    @Override public void remove() {
      throw new UnsupportedOperationException("Remove not supported on KeyIterator");
    }
  }

  class PrimitiveKeyIterator implements Iterator<Long> {

    private int curKeyIdx;
    private long[] keys;

    public PrimitiveKeyIterator(long[] keys) {
      curKeyIdx = 0;
      this.keys = keys;
    }

    @Override public boolean hasNext() {
      return (curKeyIdx < keys.length);
    }

    @Override public Long next() {
      return keys[curKeyIdx++];
    }

    @Override public void remove() {
      throw new UnsupportedOperationException("Remove not supported on KeyIterator");
    }
  }

  /**
   * Get an iterator over keys in the KV store.
   *
   * @return Iterator over keys.
   */
  Iterator<K> keyIterator();

  /**
   * Get value corresponding to key.
   *
   * @param key Input key.
   * @return Value corresponding to key.
   */
  byte[] get(K key);

  /**
   * Delete value corresponding to key.
   *
   * @param key Input key.
   * @return True if the deletion is successful; false otherwise
   */
  boolean delete(K key);

  /**
   * Get number of entries in the KV store.
   *
   * @return The number of entries in the KV store.
   */
  int numEntries();
}
