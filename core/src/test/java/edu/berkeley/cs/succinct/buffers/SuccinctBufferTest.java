package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.util.CommonUtils;
import junit.framework.TestCase;

import java.io.*;

public class SuccinctBufferTest extends TestCase {

    private String testFileRaw = this.getClass().getResource("/test_file").getFile();
    private String testFileSuccinct = this.getClass().getResource("/test_file").getFile() + ".succinct";
    private String testFileSuccinctMin = this.getClass().getResource("/test_file").getFile() + ".min.succinct";
    private String testFileSA = this.getClass().getResource("/test_file.sa").getFile();
    private String testFileISA = this.getClass().getResource("/test_file.isa").getFile();
    private String testFileNPA = this.getClass().getResource("/test_file.npa").getFile();
    private SuccinctBuffer sBuf;

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
        sBuf = new SuccinctBuffer(fileData, 3);

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
        for(int i = 0; i < sBuf.getOriginalSize(); i++) {
            long npaVal = sBuf.lookupNPA(i);
            long expected = testNPA[i];
            assertEquals(expected, npaVal);
            sum += npaVal;
            sum %= sBuf.getOriginalSize();
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
        for(int i = 0; i < sBuf.getOriginalSize(); i++) {
            long saVal = sBuf.lookupSA(i);
            assertEquals(testSA[i], saVal);
            sum += saVal;
            sum %= sBuf.getOriginalSize();
        }
        assertEquals(sum, 0);
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
        for(int i = 0; i < sBuf.getOriginalSize(); i++) {
            long isaVal = sBuf.lookupISA(i);
            assertEquals(testISA[i], isaVal);
            sum += isaVal;
            sum %= sBuf.getOriginalSize();
        }
        assertEquals(sum, 0);
    }

    /**
     * Test method: void readObject(ObjectInputStream ois)
     * Test method: void writeObject(ObjectOutputStream oos)
     *
     * @throws Exception
     */
    public void testSerializeDeserialize() throws Exception {
        System.out.println("serializeDeserialize");

        // Serialize data
        FileOutputStream fOut = new FileOutputStream(testFileSuccinct);
        ObjectOutputStream oos = new ObjectOutputStream(fOut);
        oos.writeObject(sBuf);
        oos.close();

        // Deserialize data
        FileInputStream fIn = new FileInputStream(testFileSuccinct);
        ObjectInputStream ois = new ObjectInputStream(fIn);
        SuccinctBuffer sCoreRead = (SuccinctBuffer) ois.readObject();
        ois.close();

        assertNotNull(sCoreRead);
        assertEquals(sCoreRead.getOriginalSize(), sBuf.getOriginalSize());
        for(int i = 0; i < sBuf.getOriginalSize(); i++) {
            assertEquals(sCoreRead.lookupNPA(i), sBuf.lookupNPA(i));
            assertEquals(sCoreRead.lookupSA(i), sBuf.lookupSA(i));
            assertEquals(sCoreRead.lookupISA(i), sBuf.lookupISA(i));
        }
    }

    /**
     * Test method: void writeToFile(String path)
     * Test method: void memoryMap(String path)
     *
     * @throws Exception
     */
    public void testMemoryMap() throws Exception {
        System.out.println("memoryMap");

        sBuf.writeToFile(testFileSuccinctMin);
        SuccinctBuffer sCoreRead = new SuccinctBuffer(testFileSuccinctMin, StorageMode.MEMORY_MAPPED);
        assertNotNull(sCoreRead);
        assertEquals(sCoreRead.getOriginalSize(), sBuf.getOriginalSize());
        for(int i = 0; i < sBuf.getOriginalSize(); i++) {
            assertEquals(sCoreRead.lookupNPA(i), sBuf.lookupNPA(i));
            assertEquals(sCoreRead.lookupSA(i), sBuf.lookupSA(i));
            assertEquals(sCoreRead.lookupISA(i), sBuf.lookupISA(i));
        }
    }

    /**
     * Test method: void writeToFile(String path)
     * Test method: void readFromFile(String path)
     *
     * @throws Exception
     */
    public void testReadFromFile() throws Exception {
        System.out.println("readFromFile");

        sBuf.writeToFile(testFileSuccinctMin);
        SuccinctBuffer sCoreRead = new SuccinctBuffer(testFileSuccinctMin, StorageMode.MEMORY_MAPPED);
        assertNotNull(sCoreRead);
        assertEquals(sCoreRead.getOriginalSize(), sBuf.getOriginalSize());
        for(int i = 0; i < sBuf.getOriginalSize(); i++) {
            assertEquals(sCoreRead.lookupNPA(i), sBuf.lookupNPA(i));
            assertEquals(sCoreRead.lookupSA(i), sBuf.lookupSA(i));
            assertEquals(sCoreRead.lookupISA(i), sBuf.lookupISA(i));
        }
    }
}
