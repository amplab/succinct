package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.util.SuccinctConstants;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

public class SuccinctFileStream3Test extends TestCase {

  SuccinctFileStream stream;

  private String tmpFile = this.getClass().getResource("/").getFile() + "ints.succinct";

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

    SuccinctFileBuffer buffer = new SuccinctFileBuffer(baos.toByteArray());
    buffer.writeToFile(tmpFile);
    stream = new SuccinctFileStream(new Path(tmpFile));
  }

  public void testExtracts() throws Exception {

    for (int i = 0; i < 10; i++) {
      short shortVal = stream.extractShort(i * SuccinctConstants.SHORT_SIZE_BYTES);
      assertEquals(shortVal, i);

      int intVal = stream.extractInt(10 * SuccinctConstants.SHORT_SIZE_BYTES + i * SuccinctConstants.INT_SIZE_BYTES);
      assertEquals(intVal, i);

      long longVal = stream.extractLong(10 * (SuccinctConstants.SHORT_SIZE_BYTES + SuccinctConstants.INT_SIZE_BYTES) + i * SuccinctConstants.LONG_SIZE_BYTES);
      assertEquals(longVal, i);
    }
  }
}
