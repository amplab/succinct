package edu.berkeley.cs.succinct.util.suffixarray;

import edu.berkeley.cs.succinct.util.IOUtils;
import edu.berkeley.cs.succinct.util.InputSource;
import edu.berkeley.cs.succinct.util.SuccinctConstants;
import gnu.trove.set.hash.TIntHashSet;
import junit.framework.TestCase;

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
  private int n;

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
    instance = new QSufSort();
    File inputFile = new File(testFileRaw);

    data = new byte[(int) inputFile.length()];

    DataInputStream dis = new DataInputStream(new FileInputStream(inputFile));
    dis.readFully(data);
    n = data.length + 1;

    instance.buildSuffixArray(new InputSource() {
      @Override public int length() {
        return data.length;
      }

      @Override public int get(int i) {
        return data[i];
      }
    });
  }

  public void testGetAlphabet() {
    int[] alphabet = instance.getAlphabet();

    TIntHashSet set = new TIntHashSet();
    for (byte d: data) {
      set.add(d);
    }
    set.add(SuccinctConstants.EOF);
    int[] expectedAlphabet = set.toArray();
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

    for (int i = 0; i < n; i++) {
      assertEquals(testSA[i], SA[i]);
      sum += SA[i];
      sum %= n;
    }
    assertEquals(0L, sum);
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
    for (int i = 0; i < n; i++) {
      assertEquals(testISA[i], ISA[i]);
      sum += ISA[i];
      sum %= n;
    }
    assertEquals(0L, sum);
  }
}
