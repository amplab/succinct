package edu.berkeley.cs.succinct.qsufsort;

import edu.berkeley.cs.succinct.util.CommonUtils;
import junit.framework.TestCase;

import java.io.*;

public class QSufSortTest extends TestCase {

    private QSufSort instance;
    private int fileSize;

    /**
     * Set up test.
     *
     * @throws Exception
     */
    public void setUp() throws Exception {
        super.setUp();
        instance = new QSufSort();
        File inputFile = new File("data/test_file");

        byte[] fileData = new byte[(int) inputFile.length()];
        DataInputStream dis = new DataInputStream(
                new FileInputStream(inputFile));
        dis.readFully(fileData);
        byte[] data = (new String(fileData) + (char) 1).getBytes();
        instance.buildSuffixArray(data);
        fileSize = (int) (inputFile.length() + 1);
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
        DataInputStream dIS = new DataInputStream(new FileInputStream(new File("data/test_file.sa")));
        int[] testSA = CommonUtils.readArray(dIS);
        dIS.close();
        for(int i = 0; i < fileSize; i++) {
            assertEquals(SA[i], testSA[i]);
            sum += SA[i];
            sum %= fileSize;
        }
        assertEquals(sum, 0L);
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
        DataInputStream dIS = new DataInputStream(new FileInputStream(new File("data/test_file.isa")));
        int[] testISA = CommonUtils.readArray(dIS);
        dIS.close();
        for(int i = 0; i < fileSize; i++) {
            assertEquals(ISA[i], testISA[i]);
            sum += ISA[i];
            sum %= fileSize;
        }
        assertEquals(sum, 0L);
    }
}
