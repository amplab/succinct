package edu.berkeley.cs.succinct.util.suffixarray;

import edu.berkeley.cs.succinct.util.IOUtils;
import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

public class DivSufSortTest extends TestCase {

  private String testFileRaw = this.getClass().getResource("/test_file").getFile();
  private String testFileSA = this.getClass().getResource("/test_file.sa").getFile();
  private DivSufSort instance;
  private byte[] data;

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
    instance = new DivSufSort();
    File inputFile = new File(testFileRaw);

    byte[] fileData = new byte[(int) inputFile.length()];

    DataInputStream dis = new DataInputStream(new FileInputStream(inputFile));
    dis.readFully(fileData);

    ByteArrayOutputStream out = new ByteArrayOutputStream(fileData.length + 1);
    out.write(fileData);
    out.write(0);
    data = out.toByteArray();
  }

  public void testBuildSuffixArray() throws Exception {
    instance.buildSuffixArray(data);
    int[] SA = instance.getSA();

    long sum = 0;
    DataInputStream dIS = new DataInputStream(new FileInputStream(new File(testFileSA)));
    int[] testSA = IOUtils.readArray(dIS);
    dIS.close();
    for (int i = 0; i < data.length; i++) {
      assertEquals(testSA[i], SA[i]);
      sum += SA[i];
      sum %= data.length;
    }
    assertEquals(sum, 0L);
  }
}
