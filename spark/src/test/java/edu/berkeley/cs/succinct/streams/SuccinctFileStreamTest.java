package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctFileTest;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.util.Source;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

public class SuccinctFileStreamTest extends SuccinctFileTest {
  byte[] data;
  private String testFileRaw = this.getClass().getResource("/raw.dat").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/raw.dat").getFile() + ".stream.succinct";
  private String[] queryStrings = {"int", "include", "random", "random int"};

  @Override public String getQueryString(int i) {
    return queryStrings[i];
  }

  @Override public int numQueryStrings() {
    return queryStrings.length;
  }

  @Override public String getData() {
    return new String(data);
  }

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();

    File inputFile = new File(testFileRaw);

    data = new byte[(int) inputFile.length()];
    DataInputStream dis = new DataInputStream(new FileInputStream(inputFile));
    dis.readFully(data);
    fileData = new Source() {
      @Override public int length() {
        return data.length;
      }

      @Override public int get(int i) {
        return data[i];
      }
    };

    SuccinctFileBuffer sBuf = new SuccinctFileBuffer(data);
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
