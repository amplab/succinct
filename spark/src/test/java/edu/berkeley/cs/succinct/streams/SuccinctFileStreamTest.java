package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctFileTest;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

public class SuccinctFileStreamTest extends SuccinctFileTest {
  private String testFileRaw = this.getClass().getResource("/raw.dat").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/raw.dat").getFile() + ".stream.succinct";

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();

    File inputFile = new File(testFileRaw);

    fileData = new byte[(int) inputFile.length()];
    DataInputStream dis = new DataInputStream(new FileInputStream(inputFile));
    dis.readFully(fileData);
    SuccinctFileBuffer sBuf = new SuccinctFileBuffer(fileData);
    sBuf.writeToFile(testFileSuccinct);

    sFile = new SuccinctFileStream(new Path(testFileSuccinct));
  }

  /**
   * Tear down a test.
   *
   * @throws Exception
   */
  public void tearDown() throws Exception {
    ((SuccinctFileStream) sFile).close();
  }
}
