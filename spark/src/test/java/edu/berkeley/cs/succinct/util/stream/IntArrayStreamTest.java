package edu.berkeley.cs.succinct.util.stream;

import junit.framework.TestCase;
import org.apache.hadoop.fs.FSDataInputStream;

import java.nio.IntBuffer;

public class IntArrayStreamTest extends TestCase {

  /**
   * Test method: int get(int i)
   *
   * @throws Exception
   */
  public void testGet() throws Exception {
        IntBuffer buf = IntBuffer.allocate(10);
    for (int i = 0; i < 10; i++) {
      buf.put(i);
    }
    FSDataInputStream is = TestUtils.getStream(buf);
    IntArrayStream bs = new IntArrayStream(is, 0, 40);
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
        IntArrayStream bs = new IntArrayStream(null, 0, 40);
    assertEquals(10, bs.size());
  }
}
