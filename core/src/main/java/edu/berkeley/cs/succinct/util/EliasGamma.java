package edu.berkeley.cs.succinct.util;

/**
 * Helper class for Elias Gamma encoding.
 */
public class EliasGamma {

  /**
   * Precomputed table to speed-up computation of prefix sum for delta encoded values
   * using EliasGamma encoding.
   */
  public static class PrefixSum {
    private static int[] prefixSum;

    static {
      prefixSum = new int[65536];
      for (int i = 0; i < 65536; i++) {
        int count = 0, offset = 0, sum = 0;
        while (i != 0 && offset <= 16) {
          int N = 0;
          while (BitUtils.getBit(i, offset) != 1) {
            N++;
            offset++;
          }
          offset++;
          if (offset + N <= 16) {
            sum += ((i >>> offset) & ((int) BitUtils.LOW_BITS_SET[N])) + (1 << N);
            offset += N;
            count++;
          } else {
            offset -= (N + 1);
            break;
          }
        }
        prefixSum[i] = (offset << 24) | (count << 16) | sum;
      }
    }

    /**
     * Compute the offset for the block.
     *
     * @param block The input block.
     * @return The offset encoded in the block.
     */
    public static int offset(int block) {
      return (prefixSum[block] >>> 24) & 0xFF;
    }

    /**
     * Compute the count for the block.
     *
     * @param block The input block.
     * @return The count encoded in the block.
     */
    public static int count(int block) {
      return (prefixSum[block] >>> 16) & 0xFF;
    }

    /**
     * Compute the sum for the block.
     *
     * @param block The input block.
     * @return The sum encoded in the block.
     */
    public static int sum(int block) {
      return prefixSum[block] & 0xFFFF;
    }
  }

  /**
   * The encoding size for the value if encoded using Elias Gamma encoding.
   *
   * @param value The value to encode.
   * @return The size of the encoding.
   */
  public static int encodingSize(int value) {
    return 2 * (BitUtils.bitWidth(value) - 1) + 1;
  }
}
