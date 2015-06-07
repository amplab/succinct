package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.util.CommonUtils;
import junit.framework.TestCase;

import java.io.*;

public class SuccinctCoreTest extends TestCase {

    private String testFileRaw = this.getClass().getResource("/test_file").getFile();
    private String testFileSuccinct = this.getClass().getResource("/test_file").getFile() + ".succinct";
    private String testFileSuccinctMin = this.getClass().getResource("/test_file").getFile() + ".min.succinct";
    private String testFileSA = this.getClass().getResource("/test_file.sa").getFile();
    private SuccinctCore sCore;

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
        sCore = new SuccinctCore(fileData, 3);

    }

    /**
     * Test method: long lookupNPA(long i)
     *
     * @throws Exception
     */
    public void testLookupNPA() throws Exception {
        System.out.println("lookupNPA");

        int sum = 0;
        for(int i = 0; i < sCore.getOriginalSize(); i++) {
            long npaVal = sCore.lookupNPA(i);
            long expected = sCore.lookupISA((sCore.lookupSA(i) + 1) % sCore.getOriginalSize());
            assertEquals(npaVal, expected);
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
        System.out.println("lookupSA");

        int sum = 0;
        DataInputStream dIS = new DataInputStream(new FileInputStream(new File(testFileSA)));
        int[] testSA = CommonUtils.readArray(dIS);
        dIS.close();
        for(int i = 0; i < sCore.getOriginalSize(); i++) {
            long saVal = sCore.lookupSA(i);
            assertEquals(saVal, testSA[i]);
            assertEquals(sCore.lookupISA(saVal), i);
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
        System.out.println("lookupISA");

        int sum = 0;
        for(int i = 0; i < sCore.getOriginalSize(); i++) {
            long isaVal = sCore.lookupISA(i);
            assertEquals(sCore.lookupSA(isaVal), i);
            sum += isaVal;
            sum %= sCore.getOriginalSize();
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
        oos.writeObject(sCore);
        oos.close();

        // Deserialize data
        FileInputStream fIn = new FileInputStream(testFileSuccinct);
        ObjectInputStream ois = new ObjectInputStream(fIn);
        SuccinctCore sCoreRead = (SuccinctCore) ois.readObject();
        ois.close();

        assertNotNull(sCoreRead);
        assertEquals(sCoreRead.getOriginalSize(), sCore.getOriginalSize());
        for(int i = 0; i < sCore.getOriginalSize(); i++) {
            assertEquals(sCoreRead.lookupNPA(i), sCore.lookupNPA(i));
            assertEquals(sCoreRead.lookupSA(i), sCore.lookupSA(i));
            assertEquals(sCoreRead.lookupISA(i), sCore.lookupISA(i));
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

        sCore.writeToFile(testFileSuccinctMin);
        SuccinctCore sCoreRead = new SuccinctCore(testFileSuccinctMin, StorageMode.MEMORY_MAPPED);
        assertNotNull(sCoreRead);
        assertEquals(sCoreRead.getOriginalSize(), sCore.getOriginalSize());
        for(int i = 0; i < sCore.getOriginalSize(); i++) {
            assertEquals(sCoreRead.lookupNPA(i), sCore.lookupNPA(i));
            assertEquals(sCoreRead.lookupSA(i), sCore.lookupSA(i));
            assertEquals(sCoreRead.lookupISA(i), sCore.lookupISA(i));
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

        sCore.writeToFile(testFileSuccinctMin);
        SuccinctCore sCoreRead = new SuccinctCore(testFileSuccinctMin, StorageMode.MEMORY_MAPPED);
        assertNotNull(sCoreRead);
        assertEquals(sCoreRead.getOriginalSize(), sCore.getOriginalSize());
        for(int i = 0; i < sCore.getOriginalSize(); i++) {
            assertEquals(sCoreRead.lookupNPA(i), sCore.lookupNPA(i));
            assertEquals(sCoreRead.lookupSA(i), sCore.lookupSA(i));
            assertEquals(sCoreRead.lookupISA(i), sCore.lookupISA(i));
        }
    }
}
