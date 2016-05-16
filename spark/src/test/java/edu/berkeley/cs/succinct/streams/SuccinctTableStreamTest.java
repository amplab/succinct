package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctTable;
import edu.berkeley.cs.succinct.SuccinctTableTest;
import edu.berkeley.cs.succinct.buffers.SuccinctTableBuffer;
import edu.berkeley.cs.succinct.util.Source;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

public class SuccinctTableStreamTest extends SuccinctTableTest {
  private String testFileRaw = this.getClass().getResource("/raw.dat").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/raw.dat").getFile() + ".idx.succinct";

  byte[] data;

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();

    queryTypes = new SuccinctTable.QueryType[2];
    queries = new byte[2][][];
    queryTypes[0] = SuccinctTable.QueryType.RangeSearch;
    queries[0] = new byte[][] {"/*".getBytes(), "//".getBytes()};
    queryTypes[1] = SuccinctTable.QueryType.Search;
    queries[1] = new byte[][] {"Build".getBytes()};

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
    SuccinctTableBuffer sBuf = new SuccinctTableBuffer(data, offsets);
    sBuf.writeToFile(testFileSuccinct);

    sTable = new SuccinctTableStream(new Path(testFileSuccinct));
  }

  /**
   * Tear down a test.
   *
   * @throws Exception
   */
  public void tearDown() throws Exception {
    ((SuccinctTableStream) sTable).close();
  }
}
