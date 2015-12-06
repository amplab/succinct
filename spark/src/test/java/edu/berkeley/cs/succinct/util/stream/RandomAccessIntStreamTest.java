package edu.berkeley.cs.succinct.util.stream;

import junit.framework.TestCase;
import org.apache.hadoop.fs.FSDataInputStream;

import java.nio.IntBuffer;

public class RandomAccessIntStreamTest extends TestCase {

  public void testGet() throws Exception {
        IntBuffer buf = IntBuffer.allocate(10);
    for (int i = 0; i < 10; i++) {
      buf.put(i);
    }
    FSDataInputStream is = TestUtils.getStream(buf);
    RandomAccessIntStream bs = new RandomAccessIntStream(is, 0, 40);
    for (int i = 0; i < 10; i++) {
      assertEquals(i, bs.get());
    }
  }

  public void testGet1() throws Exception {
        IntBuffer buf = IntBuffer.allocate(10);
    for (int i = 0; i < 10; i++) {
      buf.put(i);
    }
    FSDataInputStream is = TestUtils.getStream(buf);
    RandomAccessIntStream bs = new RandomAccessIntStream(is, 0, 40);
    for (int i = 0; i < 10; i++) {
      assertEquals(i, bs.get(i));
    }
  }

  public void testPosition() throws Exception {
        IntBuffer buf = IntBuffer.allocate(10);
    for (int i = 0; i < 10; i++) {
      buf.put(i);
    }
    FSDataInputStream is = TestUtils.getStream(buf);
    RandomAccessIntStream bs = new RandomAccessIntStream(is, 0, 40);
    bs.position(3);
    assertEquals(bs.position(), 3);
  }

  public void testOffsetBeginning() throws Exception {
        IntBuffer buf = IntBuffer.allocate(20);
    for (int i = 0; i < 20; i++) {
      buf.put(i);
    }
    FSDataInputStream is = TestUtils.getStream(buf);
    RandomAccessIntStream bs = new RandomAccessIntStream(is, 40, 10);
    for (int i = 10; i < 20; i++) {
      assertEquals(i, bs.get());
    }
    bs.rewind();
    assertEquals(0, bs.position());
    for (int i = 0; i < 10; i++) {
      assertEquals((i + 10), bs.get(i));
    }

  }
}
