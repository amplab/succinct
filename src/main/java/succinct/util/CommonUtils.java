package succinct.util;

public class CommonUtils {

    public static final long two32 = 1L << 32;

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

    public static int popcount(long x) {
        return Long.bitCount(x);
    }

}
