package edu.berkeley.cs.succinct.util.wavelettree;

import gnu.trove.list.array.TLongArrayList;
import junit.framework.TestCase;

public class WaveletTreeTest extends TestCase {

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test method: ByteBuffer getByteBuffer()
   *
   * @throws Exception
   */
  public void testGetByteBuffer() throws Exception {

    WaveletTree instance1 = new WaveletTree(null);
    assertNull(instance1.getByteBuffer());

    TLongArrayList A = new TLongArrayList(), B = new TLongArrayList();
    for (long i = 0L; i < 256L; i++) {
      A.add(i);
      B.add(255L - i);
    }

    WaveletTree instance2 = new WaveletTree(0L, 255L, A, B);
    assertNotNull(instance2.getByteBuffer());
  }
}
