package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.buffers.SuccinctBuffer;
import edu.berkeley.cs.succinct.util.CommonUtils;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

public class SuccinctStreamTest extends TestCase {

    private String testFileRaw = this.getClass().getResource("/raw.dat").getFile();
    private String testFileSuccinct = this.getClass().getResource("/raw.dat").getFile() + ".succinct";
    private String testFileSA = this.getClass().getResource("/raw.dat.sa").getFile();
    private String testFileISA = this.getClass().getResource("/raw.dat.isa").getFile();
    private String testFileNPA = this.getClass().getResource("/raw.dat.npa").getFile();
    private SuccinctStream sStream;

    /**
     * Set up test.
     *
     * @throws Exception
     */
    public void setUp() throws Exception {
        super.setUp();

        File inputFile = new File(testFileRaw);

        byte[] fileData = new byte[(int) inputFile.length()];
        DataInputStream dis = new DataInputStream(
                new FileInputStream(inputFile));
        dis.readFully(fileData);

        SuccinctBuffer sBuf = new SuccinctBuffer(fileData, 3);
        sBuf.writeToFile(testFileSuccinct);

        sStream = new SuccinctStream(new Path(testFileSuccinct));
    }

    /**
     * Test method: long lookupNPA(long i)
     *
     * @throws Exception
     */
    public void testLookupNPA() throws Exception {
        System.out.println("lookupNPA");

        int sum = 0;
        DataInputStream dIS = new DataInputStream(new FileInputStream(new File(testFileNPA)));
        int[] testNPA = CommonUtils.readArray(dIS);
        dIS.close();
        for(int i = 0; i < sStream.getOriginalSize(); i++) {
            long npaVal = sStream.lookupNPA(i);
            long expected = testNPA[i];
            assertEquals(expected, npaVal);
            sum += npaVal;
            sum %= sStream.getOriginalSize();
        }

        assertEquals(sum, 0);
    }

    /**
     * Test method: long lookupSA(long i)
     *
     * @throws Exception
     */
    public void testLookupSA() throws Exception {
        System.out.println("lookupSA");

        int sum = 0;
        DataInputStream dIS = new DataInputStream(new FileInputStream(new File(testFileSA)));
        int[] testSA = CommonUtils.readArray(dIS);
        dIS.close();
        for(int i = 0; i < sStream.getOriginalSize(); i++) {
            long saVal = sStream.lookupSA(i);
            assertEquals(saVal, testSA[i]);
            sum += saVal;
            sum %= sStream.getOriginalSize();
        }
        assertEquals(0, sum);
    }

    /**
     * Test method: long lookupISA(long i)
     *
     * @throws Exception
     */
    public void testLookupISA() throws Exception {
        System.out.println("lookupISA");

        int sum = 0;
        DataInputStream dIS = new DataInputStream(new FileInputStream(new File(testFileISA)));
        int[] testISA = CommonUtils.readArray(dIS);
        dIS.close();
        for(int i = 0; i < sStream.getOriginalSize(); i++) {
            long isaVal = sStream.lookupISA(i);
            assertEquals(testISA[i], isaVal);
            sum += isaVal;
            sum %= sStream.getOriginalSize();
        }
        assertEquals(sum, 0);
    }

    /**
     * Tear down a test.
     *
     * @throws Exception
     */
    public void tearDown() throws Exception {
        sStream.close();
    }
}
