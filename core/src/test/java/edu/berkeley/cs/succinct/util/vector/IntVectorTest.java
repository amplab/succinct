package edu.berkeley.cs.succinct.util.vector;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class IntVectorTest extends TestCase {

  private static final int testSize = 1024 * 1024;
  private static final int testBits = 20;

  public void testGetBitWidth() throws Exception {
    IntVector vector = new IntVector(42, 8);
    assertEquals(8, vector.getBitWidth());
  }

  public void testAddAndGet() throws Exception {
    IntVector vector = new IntVector(testSize, testBits);

    for (int i = 0; i < testSize; i++) {
      vector.add(i, i);
    }

    for (int i = 0; i < testSize; i++) {
      assertEquals(i, vector.get(i));
    }
  }

  public void testSerializeDeserializeByteBuffer() throws Exception {
    IntVector vector = new IntVector(testSize, testBits);

    for (int i = 0; i < testSize; i++) {
      vector.add(i, i);
    }

    ByteBuffer buf = ByteBuffer.allocate(vector.serializedSize());
    vector.writeToBuffer(buf);
    buf.rewind();

    IntVector vector2 = IntVector.readFromBuffer(buf);
    assert vector2 != null;
    for (int i = 0; i < testSize; i++) {
      assertEquals(vector.get(i), vector2.get(i));
    }
  }

  public void testSerializeDeserializeStream() throws Exception {
    IntVector vector = new IntVector(testSize, testBits);

    for (int i = 0; i < testSize; i++) {
      vector.add(i, i);
    }

    ByteArrayOutputStream outputByteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(outputByteStream);
    vector.writeToStream(out);

    byte[] data = outputByteStream.toByteArray();
    ByteArrayInputStream inputByteStream = new ByteArrayInputStream(data);
    DataInputStream in = new DataInputStream(inputByteStream);

    IntVector vector2 = IntVector.readFromStream(in);
    assert vector2 != null;
    for (int i = 0; i < testSize; i++) {
      assertEquals(vector.get(i), vector2.get(i));
    }
  }
}
