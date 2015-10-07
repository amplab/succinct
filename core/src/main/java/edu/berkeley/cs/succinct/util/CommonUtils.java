package edu.berkeley.cs.succinct.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class CommonUtils {

  public static final long two32 = 1L << 32;

  /**
   * Writes a portion of an array to output stream.
   *
   * @param A      Input array.
   * @param offset Offset into array.
   * @param length Length of sub-array.
   * @param os     DataOutputStream to write to.
   */
  public static void writeArray(int[] A, int offset, int length, DataOutputStream os) {
    try {
      os.writeInt(length);
      for (int i = offset; i < offset + length; i++) {
        os.writeInt(A[i]);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  /**
   * Reads an integer array from stream.
   *
   * @param is DataInputStream to read data from.
   * @return Array read from stream.
   */
  public static int[] readArray(DataInputStream is) {
    int[] A = null;

    try {
      int length = is.readInt();
      A = new int[length];
      for (int i = 0; i < length; i++) {
        A[i] = is.readInt();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return A;
  }

  /**
   * Get the integer logarithm to base 2.
   *
   * @param n Input integer.
   * @return Integer logarithm to base 2.
   */
  public static int intLog2(long n) {
    if (n < 0)
      return -1;
    int l = (n != 0) && ((n & (n - 1)) == 0) ? 0 : 1;
    while ((n >>= 1) > 0)
      ++l;
    return l;
  }

  /**
   * Get the arithmetic modulo.
   *
   * @param a Input operand.
   * @param n Modulus.
   * @return Value of the modulo.
   */
  public static long modulo(long a, long n) {
    while (a < 0)
      a += n;
    return a % n;
  }

  /**
   * Counts the number of set bits in input integer.
   *
   * @param n Input integer.
   * @return The pop-count.
   */
  public static int popCount(long n) {
    return Long.bitCount(n);
  }

  public static class DictionaryUtils {

    /**
     * Get Level 2 rank encoded in 64 bit integer.
     *
     * @param n 64 bit integer.
     * @return Level 2 rank value.
     */
    public static long GETRANKL2(long n) {
      return (n >>> 32);
    }

    /**
     * Get ith Level 1 rank encoded in 64 bit integer.
     *
     * @param n 64 bit integer.
     * @param i Index of encoded rank value.
     * @return Level 1 rank value.
     */
    public static long GETRANKL1(long n, int i) {
      return (((n & 0xffffffff) >>> (32 - i * 10)) & 0x3ff);
    }

    /**
     * Get position offset associated with Level 2 rank encoded in 64 bit integer.
     *
     * @param n 64 bit integer.
     * @return Position offset.
     */
    public static long GETPOSL2(long n) {
      return (n >>> 31);
    }

    /**
     * Get position offset associated with ith Level 1 rank encoded in 64 bit integer.
     *
     * @param n 64 bit integer.
     * @param i Index of encoded position offset.
     * @return Position offset.
     */
    public static long GETPOSL1(long n, int i) {
      return (((n & 0x7fffffff) >>> (31 - i * 10)) & 0x3ff);
    }
  }

}
