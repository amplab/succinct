package edu.berkeley.cs.succinct.wavelettree;

import edu.berkeley.cs.succinct.dictionary.Tables;
import junit.framework.TestCase;

import java.util.ArrayList;

public class WaveletTreeTest extends TestCase {

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
     * Test method: ByteBuffer getByteBuffer()
     *
     * @throws Exception
     */
    public void testGetByteBuffer() throws Exception {
        System.out.println("getByteBuffer");
        
        WaveletTree instance1 = new WaveletTree(null);
        assertNull(instance1.getByteBuffer());

        ArrayList<Long> A = new ArrayList<Long>(), B = new ArrayList<Long>();
        for(long i = 0L; i < 256L; i++) {
            A.add(i);
            B.add(255L - i);
        }
        
        WaveletTree instance2 = new WaveletTree(0L, 255L, A, B);
        assertNotNull(instance2.getByteBuffer());
    }
}
