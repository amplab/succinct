package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctFileTest;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.util.Source;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class SuccinctFileStream2Test extends SuccinctFileTest {
  char[] data;
  private String testFileRaw = this.getClass().getResource("/utf8.dat").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/utf8.dat").getFile() + ".stream.succinct";
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
