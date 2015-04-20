package edu.berkeley.cs.succinct.util;

import edu.berkeley.cs.succinct.dictionary.Tables;
import edu.berkeley.cs.succinct.wavelettree.WaveletTree;
import edu.berkeley.cs.succinct.util.SerializedOperations.WaveletTreeOps;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.nio.ByteBuffer;

public class WaveletTreeOpsTest extends TestCase {

    /**
     * Set up test.
     *
     * @throws Exception
     */
    public void setUp() throws Exception {
        super.setUp();
        Tables.init();
    }

    /**
     * Test method: long getValue()
     *
     * @throws Exception
     */
    public void testGetValue() throws Exception {
        System.out.println("getValue");

        // TODO: Fix test
        /*
        ArrayList<Long> A = new ArrayList<Long>(), B = new ArrayList<Long>();
        for(long i = 0L; i < 256L; i++) {
            A.add(i);
            B.add(255L - i);
        }

        WaveletTree wTree = new WaveletTree(0L, 255L, A, B);
        ByteBuffer wTreeBuf = wTree.getByteBuffer();
        for(int i = 0; i < 256L; i++) {
            System.out.println("i = " + i);
            long v = WaveletTreeOps.getValue(wTreeBuf, i, 0, 0, 255);
            assertEquals(v, 255L - i);

        }
        */
    }
}
