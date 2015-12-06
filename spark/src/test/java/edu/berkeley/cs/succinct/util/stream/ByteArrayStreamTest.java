package edu.berkeley.cs.succinct.util.stream;

import junit.framework.TestCase;
import org.apache.hadoop.fs.FSDataInputStream;

import java.nio.ByteBuffer;

public class ByteArrayStreamTest extends TestCase {

  /**
   * Test method: byte get(int i)
   *
   * @throws Exception
   */
  public void testGet() throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(10);
    for (int i = 0; i < 10; i++) {
      buf.put((byte) i);
    }
    FSDataInputStream is = TestUtils.getStream(buf);
    ByteArrayStream bs = new ByteArrayStream(is, 0, 10);
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
        ByteArrayStream bs = new ByteArrayStream(null, 0, 10);
    assertEquals(10, bs.size());
  }
}
