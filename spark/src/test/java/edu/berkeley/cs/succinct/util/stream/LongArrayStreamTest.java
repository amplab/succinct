package edu.berkeley.cs.succinct.util.stream;

import junit.framework.TestCase;
import org.apache.hadoop.fs.FSDataInputStream;

import java.nio.LongBuffer;

public class LongArrayStreamTest extends TestCase {

  /**
   * Test method: long get(int i)
   *
   * @throws Exception
   */
  public void testGet() throws Exception {
        LongBuffer buf = LongBuffer.allocate(10);
    for (int i = 0; i < 10; i++) {
      buf.put(i);
    }
    FSDataInputStream is = TestUtils.getStream(buf);
    LongArrayStream bs = new LongArrayStream(is, 0, 80);
    for (int i = 0; i < 10; i++) {
      assertEquals(i, bs.get(i));
    }
  }

  /**
   * Test method: int size()
   *
   * @throws Exception
   */
  public void testSize() throws Exception {
        LongArrayStream bs = new LongArrayStream(null, 0, 80);
    assertEquals(10, bs.size());
  }
}
