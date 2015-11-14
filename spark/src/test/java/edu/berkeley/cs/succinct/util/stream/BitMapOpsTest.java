package edu.berkeley.cs.succinct.util.stream;

import edu.berkeley.cs.succinct.bitmap.BitMap;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FSDataInputStream;

import java.nio.LongBuffer;
import java.util.ArrayList;

public class BitMapOpsTest extends TestCase {

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test method: long getBit(LongBuffer bitmap, int i)
   *
   * @throws Exception
   */
  public void testGetBit() throws Exception {
    System.out.println("getBit");

    BitMap instance = new BitMap(1000L);
    ArrayList<Long> test = new ArrayList<Long>();
    for (int i = 0; i < 1000; i++) {
      if ((int) (Math.random() * 2) == 1) {
        instance.setBit(i);
        test.add(1L);
      } else {
        test.add(0L);
      }
    }

    LongBuffer bBuf = instance.getLongBuffer();
    FSDataInputStream is = TestUtils.getStream(bBuf);
    RandomAccessLongStream ls = new RandomAccessLongStream(is, 0, bBuf.limit());
    for (int i = 0; i < 1000; i++) {
      long expResult = test.get(i);
      long result = SerializedOperations.BitMapOps.getBit(ls, i);
      assertEquals(expResult, result);
    }
    is.close();
  }

  /**
   * Test method: long getValPos(LongBuffer bitmap, int pos, int bits)
   *
   * @throws Exception
   */
  public void testGetValPos() throws Exception {
    System.out.println("getValPos");

    int pos = 60;
    int bits = 10;
    BitMap instance = new BitMap(1000L);
    instance.setValPos(pos, 1000, bits);

    LongBuffer bBuf = instance.getLongBuffer();
    FSDataInputStream is = TestUtils.getStream(bBuf);
    RandomAccessLongStream ls = new RandomAccessLongStream(is, 0, bBuf.limit());

    long expResult = 1000L;
    long result = SerializedOperations.BitMapOps.getValPos(ls, pos, bits);
    assertEquals(expResult, result);
    is.close();
  }

}
