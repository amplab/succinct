package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctIndexedFileTest;
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
import edu.berkeley.cs.succinct.util.Source;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

public class SuccinctIndexedFileStreamTest extends SuccinctIndexedFileTest {
  private String testFileRaw = this.getClass().getResource("/raw.dat").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/raw.dat").getFile() + ".idx.succinct";

  byte[] data;
  private String[] queryStrings = { "int", "include", "random", "random int" };
  private int[] counts = { 28, 9, 0, 0 };

  @Override public String getQueryString(int i) {
    return queryStrings[i];
  }

  @Override public int getQueryStringCount(int i) {
    return counts[i];
  }

  @Override public int numQueryStrings() {
    return queryStrings.length;
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
    ArrayList<Integer> positions = new ArrayList<>();
    positions.add(0);
    for (int i = 0; i < fileData.length(); i++) {
      if (fileData.get(i) == '\n') {
        positions.add(i + 1);
      }
    }
    offsets = new int[positions.size()];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = positions.get(i);
    }
    SuccinctIndexedFileBuffer sBuf = new SuccinctIndexedFileBuffer(data, offsets);
    sBuf.writeToFile(testFileSuccinct);

    sIFile = new SuccinctIndexedFileStream(new Path(testFileSuccinct));
  }

  /**
   * Tear down a test.
   *
   * @throws Exception
   */
  public void tearDown() throws Exception {
    ((SuccinctIndexedFileStream) sIFile).close();
  }
}
