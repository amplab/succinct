package edu.berkeley.cs.succinct.util.buffer.serops;

import junit.framework.TestCase;

import java.nio.LongBuffer;

public class ArrayOpsTest extends TestCase {

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test method: long getRank1(LongBuffer arrayBuf, int startPos, int size, long i)
   *
   * @throws Exception
   */
  public void testGetRank1() throws Exception {

    long[] data = {2L, 3L, 5L, 7L, 11L, 13L, 17L, 19L, 23L, 29L};
    LongBuffer buf = LongBuffer.wrap(data);
    assertEquals(ArrayOps.getRank1(buf, 0, data.length, 0L), 0L);
    assertEquals(ArrayOps.getRank1(buf, 0, data.length, 2L), 1L);
    assertEquals(ArrayOps.getRank1(buf, 0, data.length, 3L), 2L);
    assertEquals(ArrayOps.getRank1(buf, 0, data.length, 4L), 2L);
    assertEquals(ArrayOps.getRank1(buf, 0, data.length, 6L), 3L);
    assertEquals(ArrayOps.getRank1(buf, 0, data.length, 22L), 8L);
    assertEquals(ArrayOps.getRank1(buf, 0, data.length, 29L), 10L);
    assertEquals(ArrayOps.getRank1(buf, 0, data.length, 33L), 10L);

  }
}
