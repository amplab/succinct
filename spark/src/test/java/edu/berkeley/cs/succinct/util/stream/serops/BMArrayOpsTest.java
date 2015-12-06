package edu.berkeley.cs.succinct.util.stream.serops;

import edu.berkeley.cs.succinct.util.bitmap.BMArray;
import edu.berkeley.cs.succinct.util.stream.LongArrayStream;
import edu.berkeley.cs.succinct.util.stream.TestUtils;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FSDataInputStream;

import java.nio.LongBuffer;

public class BMArrayOpsTest extends TestCase {

  /**
   * Test method: long getVal(LongBuffer B, int i, int bits)
   *
   * @throws Exception
   */
  public void testGetVal() throws Exception {

    BMArray bmArray = new BMArray(1000, 64);
    for (int i = 0; i < 1000; i++) {
      bmArray.setVal(i, i);
    }

    LongBuffer bBuf = bmArray.getLongBuffer();
    FSDataInputStream is = TestUtils.getStream(bBuf);
    LongArrayStream ls = new LongArrayStream(is, 0, bBuf.limit() * 8);
    for (int i = 0; i < 1000; i++) {
      assertEquals(BMArrayOps.getVal(ls, i, 64), i);
    }
    is.close();
  }
}
