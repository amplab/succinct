package edu.berkeley.cs.succinct.bitmap;

import java.nio.LongBuffer;

public class BitMap {

  public long[] data;
  public long size;

  /**
   * Constructor to set up a bitmap
   *
   * @param n Number of bits in the bitmap.
   */
  public BitMap(long n) {
    long bmSize = ((n / 64) + 1);
    this.data = new long[(int) bmSize];
    this.size = n;
  }

  /**
   * Get the length of the backing bitmap array.
   *
   * @return The length of the backing bitmap array.
   */
  public int bitmapSize() {
    return this.data.length;
  }

  /**
   * Set the bit at a certain position in the bitmap.
   *
   * @param i The position of the bit.
   */
  public void setBit(int i) {
    this.data[i / 64] |= (1L << (63L - i));
  }

  /**
   * Get the bit at a certain position in the bitmap.
   *
   * @param i The position of the bit.
   * @return The value of the bit.
   */
  public long getBit(int i) {
    return ((data[i / 64] >>> (63L - i)) & 1L);
  }

  /**
   * Set the value at a certain position in the bitmap.
   *
   * @param pos  The position of the value.
   * @param val  The value to be set.
   * @param bits The size in bits of the value.
   */
  public void setValPos(int pos, long val, int bits) {

    assert (pos >= 0 && pos < this.size);
    assert (val >= 0);

    long s = pos;
    long e = s + (bits - 1);
    if ((s / 64) == (e / 64)) {
      this.data[(int) (s / 64L)] |= (val << (63L - e % 64));
    } else {
      this.data[(int) (s / 64L)] |= (val >>> (e % 64 + 1));
      this.data[(int) (e / 64L)] |= (val << (63L - e % 64));
    }
  }

  /**
   * Get the value at a certain position in the bitmap.
   *
   * @param pos  The position of the value.
   * @param bits The size in bits of the value.
   * @return The value at specified position.
   */
  public long getValPos(int pos, int bits) {
    assert (pos >= 0 && pos < (this.size));

    long val;
    long s = (long) pos;
    long e = s + (bits - 1);

    if ((s / 64) == (e / 64)) {
      val = this.data[(int) (s / 64L)] << (s % 64);
      val = val >>> (63 - e % 64 + s % 64);
    } else {
      val = this.data[(int) (s / 64L)] << (s % 64);
      val = val >>> (s % 64 - (e % 64 + 1));
      val = val | (this.data[(int) (e / 64L)] >>> (63 - e % 64));
    }
    assert (val >= 0);
    return val;
  }

  /**
   * Method to perform select1 using linear scan.
   *
   * @param i Position in the bitmap.
   * @return Value of select1 at specified position.
   */
  public long getSelect1(int i) {
    long sel = -1, count = 0;
    for (int k = 0; k < this.size; ++k) {
      if (this.getBit(k) == 1)
        count++;
      if (count == (i + 1)) {
        sel = k;
        break;
      }
    }

    return sel;
  }

  /**
   * Method to perform select0 using linear scan.
   *
   * @param i Position in the bitmap.
   * @return Value of select0 at specified position.
   */
  public long getSelect0(int i) {
    long sel = -1, count = 0;
    for (int k = 0; k < this.size; ++k) {
      if (this.getBit(k) == 0)
        count++;
      if (count == (i + 1)) {
        sel = k;
        break;
      }
    }
    return sel;
  }

  /**
   * Method to perform rank1 using linear scan.
   *
   * @param i Position in the bitmap.
   * @return Value of rank1 at specified position.
   */
  public long getRank1(int i) {
    long count = 0;
    for (int k = 0; k <= i; k++) {
      if (this.getBit(k) == 1)
        count++;
    }
    return count;
  }

  /**
   * Method to perform rank0 using linear scan.
   *
   * @param i Position in the bitmap.
   * @return Value of rank0 at specified position.
   */
  public long getRank0(int i) {
    long count = 0;
    for (int k = 0; k <= i; k++) {
      if (this.getBit(k) == 0)
        count++;
    }
    return count;
  }

  /**
   * Method to clear the contents of the bitmap.
   */
  public void clear() {
    for (int i = 0; i < data.length; i++) {
      data[i] = 0;
    }
  }

  /**
   * Serializes bitmap array data as a LongBuffer.
   *
   * @return Serialized bitmap array data as LongBuffer.
   */
  public LongBuffer getLongBuffer() {
    return LongBuffer.wrap(data);
  }
}
