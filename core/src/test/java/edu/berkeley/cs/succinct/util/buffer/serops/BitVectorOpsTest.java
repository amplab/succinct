package edu.berkeley.cs.succinct.util.buffer.serops;

import edu.berkeley.cs.succinct.util.BitUtils;
import edu.berkeley.cs.succinct.util.vector.BitVector;
import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class BitVectorOpsTest extends TestCase {

  private final int testSizeInBits = 1024 * 1024;

  public void testGetBit() throws Exception {
    BitVector bitVector = new BitVector(testSizeInBits);
    for (int i = 0; i < testSizeInBits; i++) {
      if (i % 2 == 0) {
        bitVector.setBit(i);
      }
    }

    ByteBuffer buf = ByteBuffer.allocate(bitVector.serializedSize());
    bitVector.writeToBuffer(buf);
    buf.rewind();

    int bufBlocks = buf.getInt();
    LongBuffer data = (LongBuffer) buf.slice().asLongBuffer().limit(bufBlocks);

    for (int i = 0; i < testSizeInBits; i++) {
      long value = BitVectorOps.getBit(data, i);
      long expectedValue = (i % 2) == 0 ? 1 : 0;
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

    ByteBuffer buf = ByteBuffer.allocate(bitVector.serializedSize());
    bitVector.writeToBuffer(buf);
    buf.rewind();

    int bufBlocks = buf.getInt();
    pos = 0;
    LongBuffer data = (LongBuffer) buf.slice().asLongBuffer().limit(bufBlocks);

    for (int i = 0; i < 10000; i++) {
      long value = BitVectorOps.getValue(data, pos, BitUtils.bitWidth(i));
      assertEquals(i, value);
      pos += BitUtils.bitWidth(i);
    }
  }
}
