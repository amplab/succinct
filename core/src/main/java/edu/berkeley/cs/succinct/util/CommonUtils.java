package edu.berkeley.cs.succinct.util;

import java.io.DataInputStream;
import java.io.IOException;

public class CommonUtils {

  public static final long two32 = 1L << 32;

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

}
