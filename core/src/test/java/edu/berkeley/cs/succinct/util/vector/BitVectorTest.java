package edu.berkeley.cs.succinct.util.vector;

import edu.berkeley.cs.succinct.util.BitUtils;
import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class BitVectorTest extends TestCase {

  private final int testSizeInBits = 1024 * 1024;

  public void testNumBlocks() throws Exception {
    BitVector bitVector = new BitVector(42);
    assertEquals(1, bitVector.numBlocks());
  }

  public void testSetAndGetBit() throws Exception {
    BitVector bitVector = new BitVector(testSizeInBits);
    for (int i = 0; i < testSizeInBits; i++) {
      if (i % 2 == 0) {
        bitVector.setBit(i);
      }
    }

    for (int i = 0; i < testSizeInBits; i++) {
      long value = bitVector.getBit(i);
      long expectedValue = (i % 2) == 0 ? 1 : 0;
      assertEquals(expectedValue, value);
    }
  }

  public void testClearBit() throws Exception {
    BitVector bitVector = new BitVector(testSizeInBits);
    for (int i = 0; i < testSizeInBits; i++) {
      if (i % 2 == 0) {
        bitVector.setBit(i);
      }
    }

    for (int i = 0; i < testSizeInBits; i++) {
      if (i % 4 == 0) {
        bitVector.clearBit(i);
      }
    }

    for (int i = 0; i < testSizeInBits; i++) {
      long value = bitVector.getBit(i);
      long expectedValue = (i % 2 == 0 && i % 4 != 0) ? 1 : 0;
      assertEquals(expectedValue, value);
    }
  }

  public void testGetValue() throws Exception {
    long pos = 0;
    BitVector bitVector = new BitVector(testSizeInBits);
    for (int i = 0; i < 10000; i++) {
      bitVector.setValue(pos, i, BitUtils.bitWidth(i));
      pos += BitUtils.bitWidth(i);
    }

    pos = 0;
    for (int i = 0; i < 10000; i++) {
      long value = bitVector.getValue(pos, BitUtils.bitWidth(i));
      assertEquals(i, value);
      pos += BitUtils.bitWidth(i);
    }
  }

  public void testSerializeDeserializeByteBuffer() throws Exception {
    long pos = 0;
    BitVector bitVector = new BitVector(testSizeInBits);
    for (int i = 0; i < 10000; i++) {
      bitVector.setValue(pos, i, BitUtils.bitWidth(i));
      pos += BitUtils.bitWidth(i);
    }

    ByteBuffer buf = ByteBuffer.allocate(bitVector.serializedSize());
    bitVector.writeToBuffer(buf);
    buf.rewind();

    BitVector bitVector2 = BitVector.readFromBuffer(buf);
    assert bitVector2 != null;
    pos = 0;
    for (int i = 0; i < 10000; i++) {
      long value = bitVector2.getValue(pos, BitUtils.bitWidth(i));
      long expectedValue = bitVector.getValue(pos, BitUtils.bitWidth(i));
      assertEquals(expectedValue, value);
      pos += BitUtils.bitWidth(i);
    }
  }

  public void testSerializeDeserializeStream() throws Exception {
    long pos = 0;
    BitVector bitVector = new BitVector(testSizeInBits);
    for (int i = 0; i < 10000; i++) {
      bitVector.setValue(pos, i, BitUtils.bitWidth(i));
      pos += BitUtils.bitWidth(i);
    }

    ByteArrayOutputStream outputByteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(outputByteStream);
    bitVector.writeToStream(out);

    byte[] data = outputByteStream.toByteArray();
    ByteArrayInputStream inputByteStream = new ByteArrayInputStream(data);
    DataInputStream in = new DataInputStream(inputByteStream);

    BitVector bitVector2 = BitVector.readFromStream(in);
    assert bitVector2 != null;
    pos = 0;
    for (int i = 0; i < 10000; i++) {
      long value = bitVector2.getValue(pos, BitUtils.bitWidth(i));
      long expectedValue = bitVector.getValue(pos, BitUtils.bitWidth(i));
      assertEquals(expectedValue, value);
      pos += BitUtils.bitWidth(i);
    }
  }
}
