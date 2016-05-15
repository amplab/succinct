package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctIndexedFileTest;
import edu.berkeley.cs.succinct.buffers.SuccinctTableBuffer;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

public class SuccinctTableStreamTest extends SuccinctIndexedFileTest {
  private String testFileRaw = this.getClass().getResource("/raw.dat").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/raw.dat").getFile() + ".idx.succinct";

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
    ArrayList<Integer> positions = new ArrayList<>();
    positions.add(0);
    for (int i = 0; i < fileData.length; i++) {
      if (fileData[i] == '\n') {
        positions.add(i + 1);
      }
    }
    offsets = new int[positions.size()];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = positions.get(i);
    }
    SuccinctTableBuffer sBuf = new SuccinctTableBuffer(fileData, offsets);
    sBuf.writeToFile(testFileSuccinct);

    sTFile = new SuccinctTableStream(new Path(testFileSuccinct));
  }

  /**
   * Tear down a test.
   *
   * @throws Exception
   */
  public void tearDown() throws Exception {
    ((SuccinctTableStream) sTFile).close();
  }
}
