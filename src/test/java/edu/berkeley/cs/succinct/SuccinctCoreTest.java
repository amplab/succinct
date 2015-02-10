package edu.berkeley.cs.succinct;

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
            sum += sCore.lookupNPA(i);
            sum %= sCore.getOriginalSize();
        }

        assertEquals(sum, 0);

        // TODO: Compare with pre-computed ISA
    }

    /**
     * Test method: long lookupSA(long i)
     *  
     * @throws Exception
     */
    public void testLookupSA() throws Exception {
        System.out.println("lookupSA");

        int sum = 0;
        for(int i = 0; i < sCore.getOriginalSize(); i++) {
            sum += sCore.lookupSA(i);
            sum %= sCore.getOriginalSize();
        }

        assertEquals(sum, 0);

        // TODO: Compare with pre-computed ISA
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
            sum += sCore.lookupISA(i);
            sum %= sCore.getOriginalSize();
        }
        
        assertEquals(sum, 0);
        
        // TODO: Compare with pre-computed ISA
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
