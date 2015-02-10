package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.examples.SuccinctShell;
import junit.framework.TestCase;

import java.io.*;

public class SuccinctBufferTest extends TestCase {

    private SuccinctBuffer sBuf;
    private byte[] fileData;

    /**
     * Set up test.
     *
     * @throws Exception
     */
    public void setUp() throws Exception {
        super.setUp();

        File inputFile = new File("data/test_file");

        fileData = new byte[(int) inputFile.length()];
        DataInputStream dis = new DataInputStream(
                new FileInputStream(inputFile));
        dis.readFully(fileData);
        sBuf = new SuccinctBuffer((new String(fileData) + (char) 1).getBytes(), 3);
    }

    /**
     * Test method: byte[] extract(int offset, int len)
     *
     * @throws Exception
     */
    public void testExtract() throws Exception {
        System.out.println("extract");
        
        byte[] buf1 = sBuf.extract(0, 100);
        for(int i = 0; i < 100; i++) {
            assertEquals(buf1[i], fileData[i]);
        }
        
        byte[] buf2 = sBuf.extract(fileData.length - 101, 100);
        for(int i = 0; i < 100; i++) {
            assertEquals(buf2[i], fileData[fileData.length - 101 + i]);
        }
    }

    /**
     * Test method: byte[] extractUntil(int offset, char delim)
     *  
     * @throws Exception
     */
    public void testExtractUntil() throws Exception {
        System.out.println("extractUntil");
        
        byte[] buf = sBuf.extractUntil(0, '\n');
        for(int i = 0; i < buf.length; i++) {
            assertEquals(buf[i], fileData[i]);
            assertFalse(buf[i] == '\n');
        }
        assertEquals(fileData[buf.length], '\n');
    }

    /**
     * Test method: long count(byte[] query)
     *
     * @throws Exception
     */
    public void testCount() throws Exception {
        System.out.println("count");
        
        long count1 = sBuf.count("int".getBytes());
        assertEquals(count1, 43);
        
        long count2 = sBuf.count("include".getBytes());
        assertEquals(count2, 9);
    }

    /**
     * Test method: long[] search(byte[] query)
     *
     * @throws Exception
     */
    public void testSearch() throws Exception {
        System.out.println("count");
        
        byte[] query1 = "int".getBytes();
        Long[] positions1 = sBuf.search(query1);
        for(int i = 0; i < positions1.length; i++) {
            for(int j = 0; j < query1.length; j++) {
                assertEquals(query1[j], fileData[((int) (positions1[i] + j))]);
            }
        }
        
        byte[] query2 = "include".getBytes();
        Long[] positions2 = sBuf.search(query2);
        for(int i = 0; i < positions2.length; i++) {
            for(int j = 0; j < query2.length; j++) {
                assertEquals(query2[j], fileData[((int) (positions2[i] + j))]);
            }
        }
        
    }

    /**
     * Test method: Map<Long, Integer> regexSearch(String query)
     *
     * @throws Exception
     */
    public void testRegexSearch() throws Exception {
        System.out.println("regexSearch");
        
        // TODO: Add regex search tests
    }
    
    public void testSerializeDeserialize() throws Exception {
        System.out.println("serializeDeserialize");

        // Serialize data
        FileOutputStream fOut = new FileOutputStream("data/test_file.succinct");
        ObjectOutputStream oos = new ObjectOutputStream(fOut);
        oos.writeObject(sBuf);
        oos.close();

        // Deserialize data
        FileInputStream fIn = new FileInputStream("data/test_file.succinct");
        ObjectInputStream ois = new ObjectInputStream(fIn);
        SuccinctBuffer sBufRead = (SuccinctBuffer) ois.readObject();
        ois.close();

        assertNotNull(sBufRead);
    }
}
