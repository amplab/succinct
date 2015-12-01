package edu.berkeley.cs.succinct.util.suffixarray;

import edu.berkeley.cs.succinct.util.IOUtils;
import edu.berkeley.cs.succinct.util.container.IntArray;
import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

public class SAISTest extends TestCase {

  private String testFileRaw = this.getClass().getResource("/test_file").getFile();
  private String testFileSA = this.getClass().getResource("/test_file.sa").getFile();
  private byte[] data;

  private char[] convertToCharArray(byte[] input) {
    char[] ret = new char[input.length];
    for (int i = 0; i < input.length; i++) {
      ret[i] = (char) input[i];
    }
    return ret;
  }

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

    ByteArrayOutputStream out = new ByteArrayOutputStream(fileData.length + 1);
    out.write(fileData);
    out.write(0);
    data = out.toByteArray();
  }

  public void testBuildSuffixArray1() throws Exception {
    IntArray SA = SAIS.buildSuffixArray(data);

    long sum = 0;
    DataInputStream dIS = new DataInputStream(new FileInputStream(new File(testFileSA)));
    int[] testSA = IOUtils.readArray(dIS);
    dIS.close();
    for (int i = 0; i < data.length; i++) {
      assertEquals(testSA[i], SA.get(i));
      sum += SA.get(i);
      sum %= data.length;
    }
    assertEquals(sum, 0L);
  }

  public void testBuildSuffixArray2() throws Exception {
    IntArray SA = SAIS.buildSuffixArray(convertToCharArray(data));

    long sum = 0;
    DataInputStream dIS = new DataInputStream(new FileInputStream(new File(testFileSA)));
    int[] testSA = IOUtils.readArray(dIS);
    dIS.close();
    for (int i = 0; i < data.length; i++) {
      assertEquals(testSA[i], SA.get(i));
      sum += SA.get(i);
      sum %= data.length;
    }
    assertEquals(sum, 0L);
  }

}
