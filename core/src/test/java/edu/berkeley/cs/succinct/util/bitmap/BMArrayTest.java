package edu.berkeley.cs.succinct.util.bitmap;

import junit.framework.TestCase;

public class BMArrayTest extends TestCase {

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test method: void setVal(int i, long val)
   * Test method: long getVal(int i)*
   *
   * @throws Exception
   */
  public void testSetAndGetVal() throws Exception {

    BMArray instance = new BMArray(1000, 64);

    for (int i = 0; i < 1000; i++) {
      instance.setVal(i, i);
    }

    for (int i = 0; i < 1000; i++) {
      long result = instance.getVal(i);
      assertEquals(i, result);
    }
  }

}
