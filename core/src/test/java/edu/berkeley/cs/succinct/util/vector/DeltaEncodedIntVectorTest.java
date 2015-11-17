package edu.berkeley.cs.succinct.util.vector;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class DeltaEncodedIntVectorTest extends TestCase {

  private static final int testSize = 1024 * 1024;

  public void testGetSamplingRate() throws Exception {
    int[] data = new int[1];
    DeltaEncodedIntVector vector = new DeltaEncodedIntVector(data, 128);
    assertEquals(128, vector.getSamplingRate());
  }

  public void testGet() throws Exception {

    int[] data = new int[testSize];
    for (int i = 0; i < testSize; i++) {
      data[i] = i;
    }

    DeltaEncodedIntVector vector = new DeltaEncodedIntVector(data, 128);

    for (int i = 0; i < testSize; i++) {
      assertEquals(i, vector.get(i));
    }

    DeltaEncodedIntVector vector2 = new DeltaEncodedIntVector(data, 256 * 1024, 512 * 1024, 128);

    for (int i = 256 * 1024; i < 768 * 1024; i++) {
      assertEquals(i, vector2.get(i - 256 * 1024));
    }
  }

  public void testSerializeDeserializeByteBuffer() throws Exception {
    int[] data = new int[testSize];
    for (int i = 0; i < testSize; i++) {
      data[i] = i;
    }

    DeltaEncodedIntVector vector = new DeltaEncodedIntVector(data, 128);

    ByteBuffer buf = ByteBuffer.allocate(vector.serializedSize());
    vector.writeToBuffer(buf);
    buf.rewind();

    DeltaEncodedIntVector vector2 = DeltaEncodedIntVector.readFromBuffer(buf);
    assert vector2 != null;
    for (int i = 0; i < testSize; i++) {
      assertEquals(vector.get(i), vector2.get(i));
    }
  }

  public void testSerializeDeserializeStream() throws Exception {
    int[] data = new int[testSize];
    for (int i = 0; i < testSize; i++) {
      data[i] = i;
    }

    DeltaEncodedIntVector vector = new DeltaEncodedIntVector(data, 128);

    ByteArrayOutputStream outputByteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(outputByteStream);
    vector.writeToStream(out);

    byte[] bytes = outputByteStream.toByteArray();
    ByteArrayInputStream inputByteStream = new ByteArrayInputStream(bytes);
    DataInputStream in = new DataInputStream(inputByteStream);

    DeltaEncodedIntVector vector2 = DeltaEncodedIntVector.readFromStream(in);
    assert vector2 != null;
    for (int i = 0; i < testSize; i++) {
      assertEquals(vector.get(i), vector2.get(i));
    }
  }
}
