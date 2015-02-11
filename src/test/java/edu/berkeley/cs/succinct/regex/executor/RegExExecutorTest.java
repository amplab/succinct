package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.SuccinctBuffer;
import edu.berkeley.cs.succinct.regex.parser.RegExPrimitive;
import edu.berkeley.cs.succinct.regex.parser.RegExRepeatType;
import junit.framework.TestCase;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class RegExExecutorTest extends TestCase {

    private RegExExecutor regExExecutor;
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
     * Test method: void execute()
     *
     * @throws Exception
     */
    public void testExecute() throws Exception {
        System.out.println("execute");
        
    }

    /**
     * Test method: Map<Long, Integer> mgramSearch(RegExPrimitive rp)
     *
     * @throws Exception
     */
    public void testMgramSearch() throws Exception {
        System.out.println("mgramSearch");

        String query = "int";
        RegExPrimitive regEx = new RegExPrimitive(query);
        regExExecutor = new RegExExecutor(sBuf, regEx);
        Map<Long, Integer> results = regExExecutor.mgramSearch(regEx);
        
        for(Long offset : results.keySet()) {
            int len = results.get(offset);
            for(int i = 0; i < len; i++) {
                assertEquals(fileData[offset.intValue() + i], query.charAt(i));
            }
        }
        
    }

    /**
     * Test method: Map<Long, Integer> regexUnion(Map<Long, Integer> a, Map<Long, Integer> b)
     *
     * @throws Exception
     */
    public void testRegexUnion() throws Exception {
        System.out.println("regexUnion");
        
        // Populate two maps randomly
        Map<Long, Integer> A = new TreeMap<Long, Integer>();
        Map<Long, Integer> B = new TreeMap<Long, Integer>();
        for(int i = 0; i < 100; i++) {
            Random generator = new Random();
            A.put(generator.nextLong(), generator.nextInt());
            B.put(generator.nextLong(), generator.nextInt());
        }
        
        // Check if union is correct
        regExExecutor = new RegExExecutor(null, null);
        Map<Long, Integer> unionRes = regExExecutor.regexUnion(A, B);
        
        for(Long offset : A.keySet()) {
            assertTrue(unionRes.containsKey(offset));
            assertEquals(unionRes.get(offset), A.get(offset));
        }
        
        for(Long offset : B.keySet()) {
            assertTrue(unionRes.containsKey(offset));
            assertEquals(unionRes.get(offset), B.get(offset));
        }
    }

    /**
     * Test method: Map<Long, Integer> regexConcat(Map<Long, Integer> a, Map<Long, Integer> b)
     * 
     * @throws Exception
     */
    public void testRegexConcat() throws Exception {
        System.out.println("regexConcat");

        // Populate two maps randomly
        Map<Long, Integer> A = new TreeMap<Long, Integer>();
        Map<Long, Integer> B = new TreeMap<Long, Integer>();
        for(int i = 0; i < 100; i++) {
            Random generator = new Random();
            A.put(generator.nextLong(), generator.nextInt());
            B.put(generator.nextLong(), generator.nextInt());
        }

        // Check if concat is correct
        regExExecutor = new RegExExecutor(null, null);
        Map<Long, Integer> concatRes = regExExecutor.regexConcat(A, B);
        
        for(Long offset : concatRes.keySet()) {
            assertTrue(A.containsKey(offset));
            assertTrue(B.containsKey(offset + A.get(offset)));
            int len = concatRes.get(offset);
            int expectedLen = A.get(offset) + B.get(offset + A.get(offset));
            assertEquals(len, expectedLen);
        }
    }

    /**
     * Test method: Map<Long, Integer> regexRepeat(Map<Long, Integer> a, RegExRepeatType repeatType)
     *
     * @throws Exception
     */
    public void testRegexRepeat() throws Exception {
        System.out.println("regexRepeat");

        // Populate a map randomly
        Map<Long, Integer> A = new TreeMap<Long, Integer>();
        for(int i = 0; i < 100; i++) {
            Random generator = new Random();
            A.put(generator.nextLong(), generator.nextInt());
        }

        // Check if concat is correct
        regExExecutor = new RegExExecutor(null, null);
        Map<Long, Integer> repeatRes = regExExecutor.regexRepeat(A, RegExRepeatType.OneOrMore);

        for(Long offset : repeatRes.keySet()) {
            assertTrue(A.containsKey(offset));
            int repeatLen = repeatRes.get(offset);
            int origLen = A.get(offset);
            assertTrue(repeatLen % origLen == 0);
            
            int multiple = repeatLen / origLen;
            for(int i = 1; i < multiple; i++) {
                long repOff = offset + i * repeatLen;
                assertTrue(A.containsKey(repOff));
            }
        }
    }
}
