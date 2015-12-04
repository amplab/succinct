package edu.berkeley.cs.succinct.util.buffer.serops;

import edu.berkeley.cs.succinct.util.vector.DeltaEncodedIntVector;
import junit.framework.TestCase;

import java.nio.ByteBuffer;

public class DeltaEncodedIntVectorOpsTest extends TestCase {

  private static final int testSize = 1024 * 1024;

  public void testGet() throws Exception {
    int[] data = new int[testSize];
    for (int i = 0; i < testSize; i++) {
      data[i] = i;
    }

    DeltaEncodedIntVector vector = new DeltaEncodedIntVector(data, 128);

    ByteBuffer buf = ByteBuffer.allocate(vector.serializedSize());
    vector.writeToBuffer(buf);
    buf.rewind();

    for (int i = 0; i < testSize; i++) {
      int value = DeltaEncodedIntVectorOps.get(buf, i);
      assertEquals(i, value);
    }
  }
}
