package edu.berkeley.cs.succinct.util;

import junit.framework.TestCase;

public class DictionaryUtilsTest extends TestCase {

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test method: long GETRANKL2(long n)
   *
   * @throws Exception
   */
  public void testGETRANKL2() throws Exception {
    System.out.println("GETRANKL2");

    assertEquals(CommonUtils.DictionaryUtils.GETRANKL2(0), 0);

    for (long i = 0; i < 1024; i++) {
      long rankL2 = Long.valueOf(i * (1L << 22));
      long n = rankL2 << 32;
      assertEquals(CommonUtils.DictionaryUtils.GETRANKL2(n), rankL2);
    }
  }

  /**
   * Test method: long GETRANKL1(long n)
   *
   * @throws Exception
   */
  public void testGETRANKL1() throws Exception {
    System.out.println("GETRANKL1");

    assertEquals(CommonUtils.DictionaryUtils.GETRANKL1(0, 0), 0);

    for (long i = 0; i < 1024; i++) {
      long n = 0;
      for (int j = 1; j <= 3; j++) {
        n |= (i) << (32 - j * 10);
      }
      for (int j = 1; j <= 3; j++) {
        assertEquals(CommonUtils.DictionaryUtils.GETRANKL1(n, j), i);
      }
    }
  }

  /**
   * Test method: long GETPOSL1(long n)
   *
   * @throws Exception
   */
  public void testGETPOSL2() throws Exception {
    System.out.println("GETPOSL2");

    assertEquals(CommonUtils.DictionaryUtils.GETPOSL2(0), 0);

    for (long i = 0; i < 1024; i++) {
      long posL2 = Long.valueOf(i * (1L << 22));
      long n = posL2 << 31;
      assertEquals(CommonUtils.DictionaryUtils.GETPOSL2(n), posL2);
    }
  }

  /**
   * Test method: long GETPOSL1(long n)
   *
   * @throws Exception
   */
  public void testGETPOSL1() throws Exception {
    System.out.println("GETPOSL1");

    assertEquals(CommonUtils.DictionaryUtils.GETPOSL1(0, 0), 0);

    for (long i = 0; i < 1024; i++) {
      long n = 0;
      for (int j = 1; j <= 3; j++) {
        n |= (i) << (31 - j * 10);
      }
      for (int j = 1; j <= 3; j++) {
        assertEquals(CommonUtils.DictionaryUtils.GETPOSL1(n, j), i);
      }
    }
  }
}
