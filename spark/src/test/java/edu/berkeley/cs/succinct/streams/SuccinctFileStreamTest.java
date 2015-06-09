package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

public class SuccinctFileStreamTest extends TestCase {
    private SuccinctFileStream sStream;
    private byte[] fileData;
    private String testFileRaw = this.getClass().getResource("/raw.dat").getFile();
    private String testFileSuccinct = this.getClass().getResource("/raw.dat").getFile() + ".stream.succinct";

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
        SuccinctFileBuffer sBuf = new SuccinctFileBuffer(fileData, 3);
        sBuf.writeToFile(testFileSuccinct);

        sStream = new SuccinctFileStream(new Path(testFileSuccinct));
    }

    /**
     * Test method: byte[] extract(int offset, int len)
     *
     * @throws Exception
     */
    public void testExtract() throws Exception {
        System.out.println("extract");

        byte[] buf1 = sStream.extract(0, 100);
        for(int i = 0; i < 100; i++) {
            assertEquals(buf1[i], fileData[i]);
        }

        byte[] buf2 = sStream.extract(fileData.length - 101, 100);
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

        byte[] buf = sStream.extractUntil(0, (byte) '\n');
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

        long count1 = sStream.count("int".getBytes());
        assertEquals(count1, 43);

        long count2 = sStream.count("include".getBytes());
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
        Long[] positions1 = sStream.search(query1);
        for(int i = 0; i < positions1.length; i++) {
            for(int j = 0; j < query1.length; j++) {
                assertEquals(query1[j], fileData[((int) (positions1[i] + j))]);
            }
        }

        byte[] query2 = "include".getBytes();
        Long[] positions2 = sStream.search(query2);
        for(int i = 0; i < positions2.length; i++) {
            for(int j = 0; j < query2.length; j++) {
                assertEquals(query2[j], fileData[((int) (positions2[i] + j))]);
            }
        }

    }

    /**
     * Helper method to check results for a regex query.
     *
     * @param results Results to check.
     * @param exp Expression to check against.
     * @return The check result.
     */
    private boolean checkResults(Map<Long, Integer> results, String exp) {
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
     * @param results Results to check.
     * @param exp1 First expression to check against.
     * @param exp2 Second expression to check against.
     * @return The check result.
     */
    private boolean checkResultsUnion(Map<Long, Integer> results, String exp1, String exp2) {
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
     * @param results Results to check.
     * @param exp Expression to check against.
     * @return The check result.
     */
    private boolean checkResultsRepeat(Map<Long, Integer> results, String exp) {
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
     * Test method: Map<Long, Integer> regexSearch(String query)
     *
     * @throws Exception
     */
    public void testRegexSearch() throws Exception {
        System.out.println("regexSearch");

        Map<Long, Integer> primitiveResults1 = sStream.regexSearch("c");
        assertTrue(checkResults(primitiveResults1, "c"));

        Map<Long, Integer> primitiveResults2 = sStream.regexSearch("in");
        assertTrue(checkResults(primitiveResults2, "in"));

        Map<Long, Integer> primitiveResults3 = sStream.regexSearch("out");
        assertTrue(checkResults(primitiveResults3, "out"));

        Map<Long, Integer> unionResults = sStream.regexSearch("in|out");
        assertTrue(checkResultsUnion(unionResults, "in", "out"));

        Map<Long, Integer> concatResults = sStream.regexSearch("c(in|out)");
        assertTrue(checkResultsUnion(concatResults, "cin", "cout"));

        Map<Long, Integer> repeatResults = sStream.regexSearch("c+");
        assertTrue(checkResults(repeatResults, "c"));
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
