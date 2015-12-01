package edu.berkeley.cs.succinct.util.suffixarray;

import edu.berkeley.cs.succinct.SuccinctCore;
import edu.berkeley.cs.succinct.util.IOUtils;
import gnu.trove.set.hash.TByteHashSet;
import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;

public class QSufSortTest extends TestCase {

  private String testFileRaw = this.getClass().getResource("/test_file").getFile();
  private String testFileSA = this.getClass().getResource("/test_file.sa").getFile();
  private String testFileISA = this.getClass().getResource("/test_file.isa").getFile();
  private QSufSort instance;
  private byte[] data;

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

    ByteArrayOutputStream out = new ByteArrayOutputStream(fileData.length + 1);
    out.write(fileData);
    out.write(SuccinctCore.EOF);
    data = out.toByteArray();

    instance.buildSuffixArray(data);
  }

  public void testGetAlphabet() {
    byte[] alphabet = instance.getAlphabet();

    TByteHashSet set = new TByteHashSet();
    set.addAll(data);
    byte[] expectedAlphabet = set.toArray();
    Arrays.sort(expectedAlphabet);

    assertTrue(Arrays.equals(expectedAlphabet, alphabet));
  }

  /**
   * Test method: int[] getSA()
   *
   * @throws Exception
   */
  public void testGetSA() throws Exception {
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

  /**
   * Test method: int[] getSA()
   *
   * @throws Exception
   */
  public void testGetISA() throws Exception {
    int[] ISA = instance.getISA();

    long sum = 0;
    DataInputStream dIS = new DataInputStream(new FileInputStream(new File(testFileISA)));
    int[] testISA = IOUtils.readArray(dIS);
    dIS.close();
    for (int i = 0; i < data.length; i++) {
      assertEquals(testISA[i], ISA[i]);
      sum += ISA[i];
      sum %= data.length;
    }
    assertEquals(sum, 0L);
  }
}
