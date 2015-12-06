package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.util.IOUtils;
import junit.framework.TestCase;

import java.io.DataInputStream;
import java.io.FileNotFoundException;

abstract public class SuccinctCoreTest extends TestCase {
  protected SuccinctCore sCore;

  /**
   * Get DataInputStream for precomputed NPA.
   *
   * @return DataInputStream for precomputed NPA.
   */
  protected abstract DataInputStream getNPAInputStream() throws FileNotFoundException;

  /**
   * Get DataInputStream for precomputed SA.
   *
   * @return DataInputStream for precomputed SA.
   */
  protected abstract DataInputStream getSAInputStream() throws FileNotFoundException;

  /**
   * Get DataInputStream for precomputed ISA.
   *
   * @return DataInputStream for precomputed ISA.
   */
  protected abstract DataInputStream getISAInputStream() throws FileNotFoundException;

  /**
   * Test method: long lookupNPA(long i)
   *
   * @throws Exception
   */
  public void testLookupNPA() throws Exception {

    int sum = 0;
    DataInputStream dIS = getNPAInputStream();
    int[] testNPA = IOUtils.readArray(dIS);
    dIS.close();
    for (int i = 0; i < sCore.getOriginalSize(); i++) {
      long npaVal = sCore.lookupNPA(i);
      assertEquals(testNPA[i], npaVal);
      sum += npaVal;
      sum %= sCore.getOriginalSize();
    }

    assertEquals(sum, 0);
  }

  /**
   * Test method: long lookupSA(long i)
   *
   * @throws Exception
   */
  public void testLookupSA() throws Exception {

    int sum = 0;
    DataInputStream dIS = getSAInputStream();
    int[] testSA = IOUtils.readArray(dIS);
    dIS.close();
    for (int i = 0; i < sCore.getOriginalSize(); i++) {
      long saVal = sCore.lookupSA(i);
      assertEquals(testSA[i], saVal);
      sum += saVal;
      sum %= sCore.getOriginalSize();
    }
    assertEquals(sum, 0);
  }

  /**
   * Test method: long lookupISA(long i)
   *
   * @throws Exception
   */
  public void testLookupISA() throws Exception {

    int sum = 0;
    DataInputStream dIS = getISAInputStream();
    int[] testISA = IOUtils.readArray(dIS);
    dIS.close();
    for (int i = 0; i < sCore.getOriginalSize(); i++) {
      long isaVal = sCore.lookupISA(i);
      assertEquals(testISA[i], isaVal);
      sum += isaVal;
      sum %= sCore.getOriginalSize();
    }
    assertEquals(sum, 0);
  }
}
