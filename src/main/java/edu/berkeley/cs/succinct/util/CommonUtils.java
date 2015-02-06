package edu.berkeley.cs.succinct.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

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

    public static byte[] readFile(String filePath) throws FileNotFoundException, IOException {
        File file = new File(filePath);
        FileInputStream fis = new FileInputStream(file);
        byte[] data = new byte[(int)file.length() + 1];

        fis.read(data, 0, data.length - 1);
        fis.close();

        data[data.length - 1] = 1;

        return data;
    }

}
