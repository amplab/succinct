package edu.berkeley.cs.succinct.util.buffer.serops;

import edu.berkeley.cs.succinct.util.bitmap.BitMap;
import edu.berkeley.cs.succinct.util.dictionary.Dictionary;
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
  }

  /**
   * Test method: getRank1(ByteBuffer buf, int startPos, int i)
   *
   * @throws Exception
   */
  public void testGetRank1() throws Exception {

    BitMap B = new BitMap(2048);
    for (int i = 0; i < 2048; i++) {
      if ((int) (Math.random() * 2) == 1) {
        B.setBit(i);
      }
    }

    Dictionary D = new Dictionary(B);
    ByteBuffer dBuf = D.getByteBuffer();
    for (int i = 0; i < 2048; i++) {
      assertEquals(DictionaryOps.getRank1(dBuf, 0, i), D.getRank1(i));
    }
  }

  /**
   * Test method: getRank0(ByteBuffer buf, int startPos, int i)
   *
   * @throws Exception
   */
  public void testGetRank0() throws Exception {

    BitMap B = new BitMap(2048);
    for (int i = 0; i < 2048; i++) {
      if ((int) (Math.random() * 2) == 1) {
        B.setBit(i);
      }
    }

    Dictionary D = new Dictionary(B);
    ByteBuffer dBuf = D.getByteBuffer();
    for (int i = 0; i < 2048; i++) {
      assertEquals(DictionaryOps.getRank0(dBuf, 0, i), D.getRank0(i));
    }
  }

  /**
   * Test method: getSelect1(ByteBuffer buf, int startPos, int i)
   *
   * @throws Exception
   */
  public void testGetSelect1() throws Exception {

    BitMap B = new BitMap(2048);
    ArrayList<Long> test = new ArrayList<Long>();
    for (int i = 0; i < 2048; i++) {
      if ((int) (Math.random() * 2) == 1) {
        B.setBit(i);
        test.add((long) i);
      }
    }

    Dictionary D = new Dictionary(B);
    ByteBuffer dBuf = D.getByteBuffer();
    for (int i = 0; i < test.size(); i++) {
      assertEquals(DictionaryOps.getSelect1(dBuf, 0, i), (long) test.get(i));
    }
  }

  /**
   * Test method: getSelect0(ByteBuffer buf, int startPos, int i)
   *
   * @throws Exception
   */
  public void testGetSelect0() throws Exception {

    BitMap B = new BitMap(2048);
    ArrayList<Long> test = new ArrayList<Long>();
    for (int i = 0; i < 2048; i++) {
      if ((int) (Math.random() * 2) == 1) {
        B.setBit(i);
      } else {
        test.add((long) i);
      }
    }

    Dictionary D = new Dictionary(B);
    ByteBuffer dBuf = D.getByteBuffer();
    for (int i = 0; i < test.size(); i++) {
      assertEquals(DictionaryOps.getSelect0(dBuf, 0, i), (long) test.get(i));
    }
  }
}
