package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.util.CommonUtils;
import junit.framework.TestCase;

import java.io.*;

public class SuccinctCoreTest extends TestCase {

    private SuccinctCore sCore;

    /**
     * Set up test.
     *
     * @throws Exception
     */
    public void setUp() throws Exception {
        super.setUp();

        File inputFile = new File("data/test_file");

        byte[] fileData = new byte[(int) inputFile.length()];
        DataInputStream dis = new DataInputStream(
                new FileInputStream(inputFile));
        dis.readFully(fileData);
        sCore = new SuccinctCore(
                (new String(fileData) + (char) 1).getBytes(), 3);

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
        DataInputStream dIS = new DataInputStream(new FileInputStream(new File("data/test_file.sa")));
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
        FileOutputStream fOut = new FileOutputStream("data/test_file.succinct");
        ObjectOutputStream oos = new ObjectOutputStream(fOut);
        oos.writeObject(sCore);
        oos.close();
        
        // Deserialize data
        FileInputStream fIn = new FileInputStream("data/test_file.succinct");
        ObjectInputStream ois = new ObjectInputStream(fIn);
        SuccinctCore sCoreRead = (SuccinctCore) ois.readObject();
        ois.close();
        
        assertNotNull(sCoreRead);
    }
}
