package edu.berkeley.cs.succinct.util;

import junit.framework.TestCase;

public class BitUtilsTest extends TestCase {

  public void testGetAndSetBit() throws Exception {
    long n = 0;

    for (int i = 0; i < 64; i++) {
      assertEquals(0, BitUtils.getBit(n, i));
    }

    for (int i = 0; i < 64; i++) {
      n = BitUtils.setBit(n, i);
      for (int j = 0; j < 64; j++) {
        if (j == i) {
          assertEquals(1, BitUtils.getBit(n, j));
        } else {
          assertEquals(0, BitUtils.getBit(n, j));
        }
      }
      n = 0;
    }

    for (int i = 0; i < 64; i++) {
      n = BitUtils.setBit(n, i);
      for (int j = 0; j < 64; j++) {
        if (j <= i) {
          assertEquals(1, BitUtils.getBit(n, j));
        } else {
          assertEquals(0, BitUtils.getBit(n, j));
        }
      }
    }
  }

  public void testClearBit() throws Exception {
    long N = 0;

    for (int i = 0; i < 64; i++) {
      N = BitUtils.setBit(N, i);
    }

    for (int i = 0; i < 64; i++) {
      long n = BitUtils.clearBit(N, i);
      for (int j = 0; j < 64; j++) {
        if (j == i) {
          assertEquals(0, BitUtils.getBit(n, j));
        } else {
          assertEquals(1, BitUtils.getBit(n, j));
        }
      }
    }

    for (int i = 0; i < 64; i++) {
      N = BitUtils.clearBit(N, i);
      for (int j = 0; j < 64; j++) {
        if (j <= i) {
          assertEquals(0, BitUtils.getBit(N, j));
        } else {
          assertEquals(1, BitUtils.getBit(N, j));
        }
      }
    }

  }

  public void testBitsToBlocks64() throws Exception {
    assertEquals(0, BitUtils.bitsToBlocks64(0));
    for (int i = 1; i <= 64; i++) {
      assertEquals(1, BitUtils.bitsToBlocks64(i));
    }
    assertEquals(2, BitUtils.bitsToBlocks64(65));
    assertEquals(4, BitUtils.bitsToBlocks64(256));
    assertEquals(5, BitUtils.bitsToBlocks64(257));
  }

  public void testBitWidth() throws Exception {
    assertEquals(1, BitUtils.bitWidth(0));
    assertEquals(1, BitUtils.bitWidth(1));
    assertEquals(2, BitUtils.bitWidth(2));
    assertEquals(2, BitUtils.bitWidth(3));
    assertEquals(3, BitUtils.bitWidth(4));
    // ...
    assertEquals(8, BitUtils.bitWidth(255));
    assertEquals(9, BitUtils.bitWidth(256));
  }
}
