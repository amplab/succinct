package edu.berkeley.cs.succinct.util.stream;

import junit.framework.TestCase;
import org.apache.hadoop.fs.FSDataInputStream;

import java.nio.LongBuffer;

public class RandomAccessLongStreamTest extends TestCase {

  public void testGet() throws Exception {
        LongBuffer buf = LongBuffer.allocate(10);
    for (int i = 0; i < 10; i++) {
      buf.put(i);
    }
    FSDataInputStream is = TestUtils.getStream(buf);
    RandomAccessLongStream bs = new RandomAccessLongStream(is, 0, 80);
    for (int i = 0; i < 10; i++) {
      assertEquals(i, bs.get());
    }
  }

  public void testGet1() throws Exception {
        LongBuffer buf = LongBuffer.allocate(10);
    for (int i = 0; i < 10; i++) {
      buf.put(i);
    }
    FSDataInputStream is = TestUtils.getStream(buf);
    RandomAccessLongStream bs = new RandomAccessLongStream(is, 0, 80);
    for (int i = 0; i < 10; i++) {
      assertEquals(i, bs.get(i));
    }
  }

  public void testPosition() throws Exception {
        LongBuffer buf = LongBuffer.allocate(10);
    for (int i = 0; i < 10; i++) {
      buf.put(i);
    }
    FSDataInputStream is = TestUtils.getStream(buf);
    RandomAccessLongStream bs = new RandomAccessLongStream(is, 0, 80);
    bs.position(3);
    assertEquals(bs.position(), 3);
  }

  public void testOffsetBeginning() throws Exception {
        LongBuffer buf = LongBuffer.allocate(20);
    for (int i = 0; i < 20; i++) {
      buf.put(i);
    }
    FSDataInputStream is = TestUtils.getStream(buf);
    RandomAccessLongStream bs = new RandomAccessLongStream(is, 80, 10);
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
