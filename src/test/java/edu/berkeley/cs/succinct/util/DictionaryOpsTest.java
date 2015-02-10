package edu.berkeley.cs.succinct.util;

import edu.berkeley.cs.succinct.bitmap.BitMap;
import edu.berkeley.cs.succinct.dictionary.Dictionary;
import edu.berkeley.cs.succinct.dictionary.Tables;
import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class DictionaryOpsTest extends TestCase {

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
     * Test method: getRank1(ByteBuffer buf, int startPos, int i)
     *
     * @throws Exception
     */
    public void testGetRank1() throws Exception {
        System.out.println("getRank1");

        BitMap B = new BitMap(2048);
        for(int i = 0; i < 2048; i++) {
            if((int)(Math.random() * 2) == 1) {
                B.setBit(i);
            }
        }

        Dictionary D = new Dictionary(B);
        ByteBuffer dBuf = D.getByteBuffer();
        for (int i = 0; i < 2048; i++) {
            assertEquals(SerializedOperations.DictionaryOps.getRank1(dBuf, 0, i), D.getRank1(i));
        }
    }

    /**
     * Test method: getRank0(ByteBuffer buf, int startPos, int i)
     *
     * @throws Exception
     */
    public void testGetRank0() throws Exception {
        System.out.println("getRank0");

        BitMap B = new BitMap(2048);
        for(int i = 0; i < 2048; i++) {
            if((int)(Math.random() * 2) == 1) {
                B.setBit(i);
            }
        }

        Dictionary D = new Dictionary(B);
        ByteBuffer dBuf = D.getByteBuffer();
        for (int i = 0; i < 2048; i++) {
            assertEquals(SerializedOperations.DictionaryOps.getRank0(dBuf, 0, i), D.getRank0(i));
        }
    }

    /**
     * Test method: getSelect1(ByteBuffer buf, int startPos, int i)
     *
     * @throws Exception
     */
    public void testGetSelect1() throws Exception {
        System.out.println("getSelect1");

        BitMap B = new BitMap(2048);
        ArrayList<Long> test =  new ArrayList<Long>();
        for(int i = 0; i < 2048; i++) {
            if((int)(Math.random() * 2) == 1) {
                B.setBit(i);
                test.add(Long.valueOf(i));
            }
        }

        Dictionary D =  new Dictionary(B);
        ByteBuffer dBuf = D.getByteBuffer();
        for(int i = 0; i < test.size(); i++) {
            assertEquals(SerializedOperations.DictionaryOps.getSelect1(dBuf, 0, i), test.get(i).longValue());
        }
    }

    /**
     * Test method: getSelect0(ByteBuffer buf, int startPos, int i)
     *
     * @throws Exception
     */
    public void testGetSelect0() throws Exception {
        System.out.println("getSelect0");

        BitMap B = new BitMap(2048);
        ArrayList<Long> test = new ArrayList<Long>();
        for(int i = 0; i < 2048; i++) {
            if((int)(Math.random() * 2) == 1) {
                B.setBit(i);
            } else {
                test.add(Long.valueOf(i));
            }
        }

        Dictionary D =  new Dictionary(B);
        ByteBuffer dBuf = D.getByteBuffer();
        for(int i = 0; i < test.size(); i++) {
            assertEquals(SerializedOperations.DictionaryOps.getSelect0(dBuf, 0, i), test.get(i).longValue());
        }
    }
}
