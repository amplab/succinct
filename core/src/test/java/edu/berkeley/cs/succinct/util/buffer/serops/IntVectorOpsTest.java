package edu.berkeley.cs.succinct.util.buffer.serops;

import edu.berkeley.cs.succinct.util.vector.IntVector;
import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class IntVectorOpsTest extends TestCase {

  private static final int testSize = 1024 * 1024;
  private static final int testBits = 20;

  public void testGet() throws Exception {
    IntVector vector = new IntVector(testSize, testBits);

    for (int i = 0; i < testSize; i++) {
      vector.add(i, i);
    }

    ByteBuffer buf = ByteBuffer.allocate(vector.serializedSize());
    vector.writeToBuffer(buf);
    buf.rewind();

    int bitWidth = buf.getInt();
    int bufBlocks = buf.getInt();
    LongBuffer data = (LongBuffer) buf.slice().asLongBuffer().limit(bufBlocks);

    for (int i = 0; i < 10000; i++) {
      long value = IntVectorOps.get(data, i, bitWidth);
      assertEquals(i, value);
    }
  }
}
