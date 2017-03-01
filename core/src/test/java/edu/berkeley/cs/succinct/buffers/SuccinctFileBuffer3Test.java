package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.SuccinctFile;
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
      assertEquals(i, shortVal);

      int intVal = buffer
        .extractInt(10 * SuccinctConstants.SHORT_SIZE_BYTES + i * SuccinctConstants.INT_SIZE_BYTES);
      assertEquals(i, intVal);

      long longVal = buffer.extractLong(
        10 * (SuccinctConstants.SHORT_SIZE_BYTES + SuccinctConstants.INT_SIZE_BYTES)
          + i * SuccinctConstants.LONG_SIZE_BYTES);
      assertEquals(i, longVal);
    }
  }

  public void testExtractsContiguous() throws Exception {
    SuccinctFile.ExtractContext ctx = new SuccinctFile.ExtractContext();
    short shortVal = buffer.extractShort(0, ctx);
    assertEquals(shortVal, 0);

    for (int i = 1; i < 10; i++) {
      shortVal = buffer.extractShort(ctx);
      assertEquals(i, shortVal);
    }

    for (int i = 0; i < 10; i++) {
      int intVal = buffer.extractInt(ctx);
      assertEquals(i, intVal);
    }

    for (int i = 0; i < 10; i++) {
      long longVal = buffer.extractLong(ctx);
      assertEquals(i, longVal);
    }
  }
}
