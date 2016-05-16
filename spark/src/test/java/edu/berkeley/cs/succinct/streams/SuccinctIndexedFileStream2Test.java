package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctIndexedFileTest;
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
import edu.berkeley.cs.succinct.util.Source;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class SuccinctIndexedFileStream2Test extends SuccinctIndexedFileTest {
  private String testFileRaw = this.getClass().getResource("/utf8.dat").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/utf8.dat").getFile() + ".idx.succinct";

  char[] data;
  private String[] queryStrings =
    {"kΩ", "əsoʊsiˈeıʃn", "‘single’", "გაიაროთ", "в", "ร", "ተ", "ᚻᛖ", "⡌⠁", "╳", "rand"};

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

    data = new char[(int) inputFile.length()];
    InputStreamReader inputReader = new InputStreamReader(new FileInputStream(inputFile), "UTF8");
    inputReader.read(data, 0, data.length);
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
