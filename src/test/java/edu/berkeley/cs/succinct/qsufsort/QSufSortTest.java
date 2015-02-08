package edu.berkeley.cs.succinct.qsufsort;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;

public class QSufSortTest extends TestCase {

    private QSufSort instance;

    /**
     * Set up test.
     *
     * @throws Exception
     */
    public void setUp() throws Exception {
        super.setUp();
        instance = new QSufSort();
        File file = new File("data/test_file");
        byte[] data = new byte[(int) file.length()];
        try {
            new FileInputStream(file).read(data);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue("Could not read from data/test_file.", false);
        }
        instance.buildSuffixArray(data);
    }

    /**
     * Test method: MinMax minmax(byte[] input)
     *  
     * @throws Exception
     */
    public void testMinmax() throws Exception {
        byte[] input = new byte[100];
        int expectedMin = 20;
        int expectedMax = 119;
        int expectedRange = 99;
        for(int i = 0; i < input.length; i++) {
            input[i] = (byte) (i + 20);
        }
        
        QSufSort.MinMax mm = QSufSort.minmax(input);
        assertEquals(mm.min, expectedMin);
        assertEquals(mm.max, expectedMax);
        assertEquals(mm.range(), expectedRange);
    }

    /**
     * Test method: int[] getSA()
     *  
     * @throws Exception
     */
    public void testGetSA() throws Exception {
        System.out.println("getSA");
        int[] SA = instance.getSA();
        
        long sum = 0;
        for(int i = 0; i < SA.length; i++) {
            sum += SA[i];
            sum %= SA.length;
        }
        assertEquals(sum, 0L);

        // TODO: Add precomputed SA check
    }

    /**
     * Test method: int[] getISA()
     *
     * @throws Exception
     */
    public void testGetSAinv() throws Exception {
        System.out.println("getISA");
        int[] ISA = instance.getISA();
        
        long sum = 0;
        for(int i = 0; i < ISA.length; i++) {
            sum += ISA[i];
            sum %= ISA.length;
        }
        assertEquals(sum, 0L);

        // TODO: Add precomputed SAinv check
    }
}
