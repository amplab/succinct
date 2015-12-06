package edu.berkeley.cs.succinct.util;

import junit.framework.TestCase;

public class CommonUtilsTest extends TestCase {

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test method: int intLog2(long n)
   *
   * @throws Exception
   */
  public void testIntLog2() throws Exception {

    assertEquals(CommonUtils.intLog2(0L), 1);
    assertEquals(CommonUtils.intLog2(1L), 0);
    assertEquals(CommonUtils.intLog2(2L), 1);
    assertEquals(CommonUtils.intLog2(3L), 2);
    assertEquals(CommonUtils.intLog2(4L), 2);
    assertEquals(CommonUtils.intLog2(5L), 3);
    assertEquals(CommonUtils.intLog2(6L), 3);
    assertEquals(CommonUtils.intLog2(99L), 7);
    assertEquals(CommonUtils.intLog2(-5), -1);
  }

  /**
   * Test method: long modulo(long a, long n)
   *
   * @throws Exception
   */
  public void testModulo() throws Exception {

    assertEquals(CommonUtils.modulo(-2, 3), 1);
    assertEquals(CommonUtils.modulo(5, 2), 1);
    assertEquals(CommonUtils.modulo(13, 13), 0);
    assertEquals(CommonUtils.modulo(15, 17), 15);
  }

  /**
   * Test method: int popCount(long x)
   *
   * @throws Exception
   */
  public void testPopCount() throws Exception {

    assertEquals(CommonUtils.popCount(0L), 0);
    assertEquals(CommonUtils.popCount(~0L), 64);
    assertEquals(CommonUtils.popCount(0xFFFF0000L), 16);
    assertEquals(CommonUtils.popCount(~1L), 63);
  }

  public void testNumBlocks() throws Exception {
    assertEquals(0, CommonUtils.numBlocks(0, 5));
    for (int i = 1; i <= 5; i++) {
      assertEquals(1, CommonUtils.numBlocks(i, 5));
    }
    assertEquals(2, CommonUtils.numBlocks(6, 5));
    assertEquals(52, CommonUtils.numBlocks(256, 5));
    assertEquals(51, CommonUtils.numBlocks(255, 5));
  }
}
