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

  public void testBinarySearch() throws Exception {
    int[] data = new int[testSize];
    for (int i = 0; i < testSize; i++) {
      data[i] = i;
    }

    DeltaEncodedIntVector vector1 = new DeltaEncodedIntVector(data, 128);

    ByteBuffer buf1 = ByteBuffer.allocate(vector1.serializedSize());
    vector1.writeToBuffer(buf1);
    buf1.rewind();

    for (int i = 0; i < testSize; i++) {
      int value = DeltaEncodedIntVectorOps.binarySearch(buf1, i, 0, testSize - 1, true);
      assertEquals(i, value);
    }

    for (int i = 0; i < testSize; i++) {
      data[i] = i * 2;
    }

    DeltaEncodedIntVector vector2 = new DeltaEncodedIntVector(data, 128);

    ByteBuffer buf2 = ByteBuffer.allocate(vector2.serializedSize());
    vector2.writeToBuffer(buf2);
    buf2.rewind();

    for (int i = 0; i < testSize - 1; i++) {
      int lo = DeltaEncodedIntVectorOps.binarySearch(buf2, 2 * i + 1, 0, testSize - 1, true);
      int hi = DeltaEncodedIntVectorOps.binarySearch(buf2, 2 * i + 1, 0, testSize - 1, false);
      assertEquals(i, lo);
      assertEquals(i + 1, hi);
    }
  }
}
