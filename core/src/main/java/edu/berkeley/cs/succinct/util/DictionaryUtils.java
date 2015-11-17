package edu.berkeley.cs.succinct.util;

public class DictionaryUtils {

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
    return ((n >>> (32 - i * 10)) & 0x3ff);
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
