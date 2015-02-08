package edu.berkeley.cs.succinct.util;

public class CommonUtils {

    public static final long two32 = 1L << 32;
    
    public static class DictionaryUtils {

        public static long GETRANKL2(long n) {
            return (n >>> 32);
        }

        public static long GETRANKL1(long n, int i) {
            return (((n & 0xffffffff) >>> (32 - i * 10)) & 0x3ff);
        }

        public static long GETPOSL2(long n) {
            return (n >>> 31);
        }

        public static long GETPOSL1(long n, int i) {
            return (((n & 0x7fffffff) >>> (31 - i * 10)) & 0x3ff);
        }
    }

    public static int intLog2(long n) {
        int l = (n != 0) && ((n & (n - 1)) == 0) ? 0 : 1;
        while ((n >>= 1) > 0)
            ++l;
        return l;
    }

    public static long modulo(long a, long n) {
        while (a < 0)
            a += n;
        return a % n;
    }

    public static int popCount(long x) {
        return Long.bitCount(x);
    }

}
