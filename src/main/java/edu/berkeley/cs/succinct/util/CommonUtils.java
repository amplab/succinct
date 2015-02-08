package edu.berkeley.cs.succinct.util;

public class CommonUtils {

    public static final long two32 = 1L << 32;
    
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

    /**
     * Get the integer logarithm to base 2.
     *
     * @param n Input integer.
     * @return Integer logarithm to base 2.
     */
    public static int intLog2(long n) {
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
     * @param x Input integer.
     * @return The pop-count.
     */
    public static int popCount(long x) {
        return Long.bitCount(x);
    }

}
