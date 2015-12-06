package edu.berkeley.cs.succinct.util.bitmap;

import junit.framework.TestCase;

import java.nio.LongBuffer;
import java.util.ArrayList;

public class BitMapTest extends TestCase {

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test method: void bitmapSize()
   *
   * @throws Exception
   */
  public void testBitmapSize() throws Exception {

    BitMap instance = new BitMap(1000L);
    int expectedSize = (1000 / 64) + 1;
    assertEquals(instance.bitmapSize(), expectedSize);
  }

  /**
   * Test method: void setBit(int pos)
   * Test method: long getBit(int pos)
   *
   * @throws Exception
   */
  public void testSetAndGetBit() throws Exception {

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

    for (int i = 0; i < 1000; i++) {
      long expResult = test.get(i);
      long result = instance.getBit(i);
      assertEquals(expResult, result);
    }
  }

  /**
   * Test method: void setValPos(int pos, long val, int bits)
   * Test method: long getValPos(int pos, int bits)
   *
   * @throws Exception
   */
  public void testSetAndGetValPos() throws Exception {

    int pos = 60;
    int bits = 10;
    BitMap instance = new BitMap(1000L);
    instance.setValPos(pos, 1000, bits);
    long expResult = 1000L;
    long result = instance.getValPos(pos, bits);
    assertEquals(expResult, result);
  }

  /**
   * Test method: long getSelect1(int i)
   *
   * @throws Exception
   */
  public void testGetSelect1() throws Exception {

    BitMap instance = new BitMap(2048);
    ArrayList<Long> test = new ArrayList<Long>();
    for (int i = 0; i < 2048; i++) {
      if ((int) (Math.random() * 2) == 1) {
        instance.setBit(i);
        test.add((long) i);
      }
    }

    for (int i = 0; i < test.size(); i++) {
      assertEquals(instance.getSelect1(i), (long)test.get(i));
    }
  }

  /**
   * Test method: long getSelect0(int i)
   *
   * @throws Exception
   */
  public void testGetSelect0() throws Exception {

    BitMap instance = new BitMap(2048);
    ArrayList<Long> test = new ArrayList<Long>();
    for (int i = 0; i < 2048; i++) {
      if ((int) (Math.random() * 2) == 1) {
        instance.setBit(i);
      } else {
        test.add((long) i);
      }
    }

    for (int i = 0; i < test.size(); i++) {
      assertEquals(instance.getSelect0(i), (long) test.get(i));
    }
  }

  /**
   * Test method: long getRank1(int i)
   *
   * @throws Exception
   */
  public void testGetRank1() throws Exception {

    BitMap instance = new BitMap(2048);
    ArrayList<Long> test = new ArrayList<Long>();
    long count = 0;
    for (int i = 0; i < 2048; i++) {
      if ((int) (Math.random() * 2) == 1) {
        instance.setBit(i);
        count++;
      }
      test.add(count);
    }

    for (int i = 0; i < test.size(); i++) {
      assertEquals(instance.getRank1(i), (long) test.get(i));
    }
  }

  /**
   * Test method: long getRank0(int i)
   *
   * @throws Exception
   */
  public void testGetRank0() throws Exception {

    BitMap instance = new BitMap(2048);
    ArrayList<Long> test = new ArrayList<Long>();
    long count = 0;
    for (int i = 0; i < 2048; i++) {
      if ((int) (Math.random() * 2) == 1) {
        instance.setBit(i);
      } else {
        count++;
      }
      test.add(count);
    }

    for (int i = 0; i < test.size(); i++) {
      assertEquals(instance.getRank0(i), (long) test.get(i));
    }
  }

  /**
   * Test method: void clear()
   *
   * @throws Exception
   */
  public void testClear() throws Exception {

    BitMap instance = new BitMap(1000L);
    for (int i = 0; i < 1000; i++) {
      if ((int) (Math.random() * 2) == 1) {
        instance.setBit(i);
      }
    }

    instance.clear();

    for (int i = 0; i < 1000; i++) {
      assertEquals(instance.getBit(i), 0L);
    }
  }

  /**
   * Test method: LongBuffer getLongBuffer()
   *
   * @throws Exception
   */
  public void testGetLongBuffer() throws Exception {

    BitMap instance = new BitMap(1000L);
    for (int i = 0; i < 1000; i++) {
      if ((int) (Math.random() * 2) == 1) {
        instance.setBit(i);
      }
    }

    LongBuffer bBuf = instance.getLongBuffer();
    assertNotNull(bBuf);

    for (int i = 0; i < instance.bitmapSize(); i++) {
      assertEquals(bBuf.get(i), instance.data[i]);
    }
  }

}
