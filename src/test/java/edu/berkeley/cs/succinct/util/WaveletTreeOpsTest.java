package edu.berkeley.cs.succinct.util;

import edu.berkeley.cs.succinct.dictionary.Tables;
import junit.framework.TestCase;

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
    public void testGetValueWtree() throws Exception {
        System.out.println("getValue");

        // TODO: Fix test
        /*
        ArrayList<Long> A = new ArrayList<Long>(), B = new ArrayList<Long>();
        for(int i = 0; i < 1000; i++) {
            A.add((long)i);
            B.add((long)(1000 - i - 1));
        }

        WaveletTree wTree = new WaveletTree(0L, 999L, A, B);
        ByteBuffer wTreeBuf = wTree.getByteBuffer();
        for(int i = 0; i < 1000; i++) {
            System.out.println("i = " + i);
            long v = SerializedOperations.getValue(wTreeBuf, i, 0, 0, 999);
            assertEquals(v, 1000 - i - 1);
        }
        */
    }
}
