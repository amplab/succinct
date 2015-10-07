package edu.berkeley.cs.succinct.qsufsort;

import edu.berkeley.cs.succinct.util.CommonUtils;
import junit.framework.TestCase;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

public class QSufSortTest extends TestCase {

  private String testFileRaw = this.getClass().getResource("/test_file").getFile();
  private String testFileSA = this.getClass().getResource("/test_file.sa").getFile();
  private String testFileISA = this.getClass().getResource("/test_file.isa").getFile();
  private QSufSort instance;
  private int fileSize;

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
    instance = new QSufSort();
    File inputFile = new File(testFileRaw);

    byte[] fileData = new byte[(int) inputFile.length()];

    DataInputStream dis = new DataInputStream(new FileInputStream(inputFile));
    dis.readFully(fileData);
    byte[] data = (new String(fileData) + (char) 1).getBytes();

    instance.buildSuffixArray(data);
    fileSize = (int) (inputFile.length() + 1);
  }

  /**
   * Test method: MinMax minmax(byte[] input)
   *
   * @throws Exception
   */
  public void testMinmax() throws Exception {
    byte[] input = new byte[100];
    int expectedMin = 20;
    int expectedMax = 119;
    int expectedRange = 99;
    for (int i = 0; i < input.length; i++) {
      input[i] = (byte) (i + 20);
    }

    QSufSort.MinMax mm = QSufSort.minmax(input);
    assertEquals(mm.min, expectedMin);
    assertEquals(mm.max, expectedMax);
    assertEquals(mm.range(), expectedRange);
  }

  /**
   * Test method: int[] getSA()
   *
   * @throws Exception
   */
  public void testGetSA() throws Exception {
    System.out.println("getSA");
    int[] SA = instance.getSA();

    long sum = 0;
    DataInputStream dIS = new DataInputStream(new FileInputStream(new File(testFileSA)));
    int[] testSA = CommonUtils.readArray(dIS);
    dIS.close();
    for (int i = 0; i < fileSize; i++) {
      assertEquals(SA[i], testSA[i]);
      sum += SA[i];
      sum %= fileSize;
    }
    assertEquals(sum, 0L);
  }
}
