package edu.berkeley.cs.succinct.util;

import edu.berkeley.cs.succinct.bitmap.BMArray;
import edu.berkeley.cs.succinct.bitmap.BitMap;
import edu.berkeley.cs.succinct.dictionary.Dictionary;
import edu.berkeley.cs.succinct.dictionary.Tables;
import edu.berkeley.cs.succinct.wavelettree.WaveletTree;
import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;

public class SerializedOperationsTest extends TestCase {

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
     * Test method: long getValueWtree()
     *
     * @throws Exception
     */
    public void testGetValueWtree() throws Exception {
        System.out.println("getValueWtree");

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
            long v = SerializedOperations.getValueWtree(wTreeBuf, i, 0, 0, 999);
            assertEquals(v, 1000 - i - 1);
        }
        */
    }

    public void testGetRank11() throws Exception {
        System.out.println("getRank11");

        long[] data = {2L, 3L, 5L, 7L, 11L, 13L, 17L, 19L, 23L, 29L};
        LongBuffer buf = LongBuffer.wrap(data);
        assertEquals(SerializedOperations.getRank1(buf, 0, data.length, 0L), 0L);
        assertEquals(SerializedOperations.getRank1(buf, 0, data.length, 2L), 1L);
        assertEquals(SerializedOperations.getRank1(buf, 0, data.length, 3L), 2L);
        assertEquals(SerializedOperations.getRank1(buf, 0, data.length, 4L), 2L);
        assertEquals(SerializedOperations.getRank1(buf, 0, data.length, 6L), 3L);
        assertEquals(SerializedOperations.getRank1(buf, 0, data.length, 22L), 8L);
        assertEquals(SerializedOperations.getRank1(buf, 0, data.length, 29L), 10L);
        assertEquals(SerializedOperations.getRank1(buf, 0, data.length, 33L), 10L);

    }

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
            assertEquals(SerializedOperations.getRank1(dBuf, 0, i), D.getRank1(i));
        }
    }

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
            assertEquals(SerializedOperations.getRank0(dBuf, 0, i), D.getRank0(i));
        }
    }

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
            assertEquals(SerializedOperations.getSelect1(dBuf, 0, i), test.get(i).longValue());
        }
    }

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
            assertEquals(SerializedOperations.getSelect0(dBuf, 0, i), test.get(i).longValue());
        }
    }

    public void testGetVal() throws Exception {
        System.out.println("getVal");

        BMArray bmArray = new BMArray(1000, 64);
        for(int i = 0; i < 1000; i++) {
            bmArray.setVal(i, i);
        }
        
        LongBuffer bBuf = bmArray.getLongBuffer();
        for(int i = 0; i < 1000; i++) {
            assertEquals(SerializedOperations.getVal(bBuf, i, 64), i);
        }
        
    }
    
    public void testGetValPos() throws Exception {
        System.out.println("getValPos");

        int pos = 60;
        int bits = 10;
        BitMap instance = new BitMap(1000L);
        instance.setValPos(pos, 1000, bits);
        LongBuffer bBuf = instance.getLongBuffer();
        long expResult = 1000L;
        long result = SerializedOperations.getValPos(bBuf, pos, bits);
        assertEquals(expResult, result);
    }

    public void testGetBit() throws Exception {
        System.out.println("getBit");

        BitMap instance = new BitMap(1000L);
        ArrayList<Long> test = new ArrayList<Long>();
        for(int i = 0; i < 1000; i++) {
            if((int)(Math.random() * 2) == 1) {
                instance.setBit(i);
                test.add(1L);
            } else {
                test.add(0L);
            }
        }

        LongBuffer bBuf = instance.getLongBuffer();
        for(int i = 0; i < 1000; i++) {
            long expResult = test.get(i);
            long result = SerializedOperations.getBit(bBuf, i);
            assertEquals(expResult, result);
        }
    }

}
