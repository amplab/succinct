package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctCoreTest;
import edu.berkeley.cs.succinct.buffers.SuccinctBuffer;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class SuccinctStream2Test extends SuccinctCoreTest {

  private String testFileRaw = this.getClass().getResource("/utf8.dat").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/utf8.dat").getFile() + ".succinct";
  private String testFileSA = this.getClass().getResource("/utf8.dat.sa").getFile();
  private String testFileISA = this.getClass().getResource("/utf8.dat.isa").getFile();
  private String testFileNPA = this.getClass().getResource("/utf8.dat.npa").getFile();

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();

    File inputFile = new File(testFileRaw);

    char[] fileData = new char[(int) inputFile.length()];
    InputStreamReader inputReader = new InputStreamReader(new FileInputStream(inputFile), "UTF8");
    inputReader.read(fileData, 0, fileData.length);

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
