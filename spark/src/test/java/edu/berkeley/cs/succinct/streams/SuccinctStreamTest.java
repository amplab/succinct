package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctCoreTest;
import edu.berkeley.cs.succinct.buffers.SuccinctBuffer;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class SuccinctStreamTest extends SuccinctCoreTest {

  private String testFileRaw = this.getClass().getResource("/raw.dat").getFile();
  private String testFileSuccinct = this.getClass().getResource("/raw.dat").getFile() + ".succinct";
  private String testFileSA = this.getClass().getResource("/raw.dat.sa").getFile();
  private String testFileISA = this.getClass().getResource("/raw.dat.isa").getFile();
  private String testFileNPA = this.getClass().getResource("/raw.dat.npa").getFile();

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();

    File inputFile = new File(testFileRaw);

    byte[] fileData = new byte[(int) inputFile.length()];
    DataInputStream dis = new DataInputStream(new FileInputStream(inputFile));
    dis.readFully(fileData);

    SuccinctBuffer sBuf = new SuccinctBuffer(fileData);
    sBuf.writeToFile(testFileSuccinct);

    sCore = new SuccinctStream(new Path(testFileSuccinct));
  }

  @Override protected DataInputStream getNPAInputStream() throws FileNotFoundException {
    return new DataInputStream(new FileInputStream(new File(testFileNPA)));
  }

  @Override protected DataInputStream getSAInputStream() throws FileNotFoundException {
    return new DataInputStream(new FileInputStream(new File(testFileSA)));
  }

  @Override protected DataInputStream getISAInputStream() throws FileNotFoundException {
    return new DataInputStream(new FileInputStream(new File(testFileISA)));
  }

  /**
   * Tear down a test.
   *
   * @throws Exception
   */
  public void tearDown() throws Exception {
    ((SuccinctStream) sCore).close();
  }
}
