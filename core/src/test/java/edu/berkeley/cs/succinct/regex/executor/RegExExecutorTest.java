package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.SuccinctBuffer;
import edu.berkeley.cs.succinct.regex.parser.*;
import junit.framework.TestCase;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class RegExExecutorTest extends TestCase {

    private String testFileRaw = this.getClass().getResource("/test_file").getFile();
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
        File inputFile = new File(testFileRaw);

        fileData = new byte[(int) inputFile.length()];
        DataInputStream dis = new DataInputStream(
                new FileInputStream(inputFile));
        dis.readFully(fileData);
        sBuf = new SuccinctBuffer(fileData, 3);
    }

    /**
     * Helper method to check results for a regex query.
     *
     * @param regEx Input regular expression.
     * @param exp Expression to check against.
     * @return The check result.
     */
    private boolean checkResults(RegEx regEx, String exp) {
        Map<Long, Integer> results;
        
        regExExecutor = new RegExExecutor(sBuf, regEx);
        regExExecutor.execute();
        results = regExExecutor.getFinalResults();
        for(Long offset : results.keySet()) {
            for(int i = 0; i < exp.length(); i++) {
                if(fileData[offset.intValue() + i] != exp.charAt(i)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Helper method to check results for a union query.
     *
     * @param regEx Input regular expression.
     * @param exp1 First expression to check against.
     * @param exp2 Second expression to check against.
     * @return The check result.
     */
    private boolean checkResultsUnion(RegEx regEx, String exp1, String exp2) {
        Map<Long, Integer> results;

        regExExecutor = new RegExExecutor(sBuf, regEx);
        regExExecutor.execute();
        results = regExExecutor.getFinalResults();
        for(Long offset : results.keySet()) {
            boolean flagFirst = true;
            boolean flagSecond = true;
            for(int i = 0; i < exp1.length(); i++) {
                if(fileData[offset.intValue() + i] != exp1.charAt(i)) {
                    flagFirst = false;
                }
            }

            for(int i = 0; i < exp2.length(); i++) {
                if(fileData[offset.intValue() + i] != exp2.charAt(i)) {
                    flagSecond = false;
                }
            }
            
            if(!flagFirst && !flagSecond) return false;
        }
        return true;
    }

    /**
     * Helper method to check results for a regex repeat query.
     *
     * @param regEx Input regular expression.
     * @param exp Expression to check against.
     * @return The check result.
     */
    private boolean checkResultsRepeat(RegEx regEx, String exp) {
        Map<Long, Integer> results;

        regExExecutor = new RegExExecutor(sBuf, regEx);
        regExExecutor.execute();
        results = regExExecutor.getFinalResults();
        for(Long offset : results.keySet()) {
            for(int i = 0; i < results.get(offset); i++) {
                if(fileData[offset.intValue() + i] != exp.charAt(i % exp.length())) {
                    return false;
                }
            }
        }
        return true;
    }
    
    /**
     * Test method: void execute()
     *
     * @throws Exception
     */
    public void testExecute() throws Exception {
        System.out.println("execute");

        RegEx primitive1 = new RegExPrimitive("c");
        RegEx primitive2 = new RegExPrimitive("out");
        RegEx primitive3 = new RegExPrimitive("in");
        RegEx union = new RegExUnion(primitive2, primitive3);
        RegEx concat = new RegExConcat(primitive1, primitive2);
        RegEx repeat = new RegExRepeat(primitive1, RegExRepeatType.OneOrMore);
        
        // Check primitives
        assertTrue(checkResults(primitive1, "c"));
        assertTrue(checkResults(primitive2, "out"));
        assertTrue(checkResults(primitive3, "in"));
        
        // Check operators
        assertTrue(checkResultsUnion(union, "in", "out"));
        assertTrue(checkResults(concat, "cout"));
        assertTrue(checkResultsRepeat(repeat, "c"));
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
