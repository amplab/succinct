package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.util.SuccinctConstants;
import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

public class SuccinctFileBuffer3Test extends TestCase {

  SuccinctFileBuffer buffer;

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    for (int i = 0; i < 10; i++) {
      dos.writeShort(i);
    }

    for (int i = 0; i < 10; i++) {
      dos.writeInt(i);
    }

    for (int i = 0; i < 10; i++) {
      dos.writeLong(i);
    }

    buffer = new SuccinctFileBuffer(baos.toByteArray());
  }

  public void testExtracts() throws Exception {

    for (int i = 0; i < 10; i++) {
      short shortVal = buffer.extractShort(i * SuccinctConstants.SHORT_SIZE_BYTES);
      assertEquals(shortVal, i);

      int intVal = buffer
        .extractInt(10 * SuccinctConstants.SHORT_SIZE_BYTES + i * SuccinctConstants.INT_SIZE_BYTES);
      assertEquals(intVal, i);

      long longVal = buffer.extractLong(
        10 * (SuccinctConstants.SHORT_SIZE_BYTES + SuccinctConstants.INT_SIZE_BYTES)
          + i * SuccinctConstants.LONG_SIZE_BYTES);
      assertEquals(longVal, i);
    }
  }
}
