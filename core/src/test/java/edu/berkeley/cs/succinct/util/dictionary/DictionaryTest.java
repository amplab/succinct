package edu.berkeley.cs.succinct.util.dictionary;

import edu.berkeley.cs.succinct.util.bitmap.BitMap;
import junit.framework.TestCase;

import java.nio.ByteBuffer;

public class DictionaryTest extends TestCase {

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test method: long getRank1(int i)
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
    Dictionary instance = new Dictionary(B);
    for (int i = 0; i < 2048; i++) {
      assertEquals(B.getRank1(i), instance.getRank1(i));
    }
  }

  /**
   * Test method: long getRank0(int i)
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
    Dictionary instance = new Dictionary(B);
    for (int i = 0; i < 2048; i++) {
      assertEquals(B.getRank0(i), instance.getRank0(i));
    }
  }

  /**
   * Test method: ByteBuffer getByteBuffer()
   *
   * @throws Exception
   */
  public void testGetByteBuffer() throws Exception {

    BitMap B = new BitMap(2048);
    for (int i = 0; i < 2048; i++) {
      if ((int) (Math.random() * 2) == 1) {
        B.setBit(i);
      }
    }

    ByteBuffer instance = new Dictionary(B).getByteBuffer();
    assertNotNull(instance);
  }
}
