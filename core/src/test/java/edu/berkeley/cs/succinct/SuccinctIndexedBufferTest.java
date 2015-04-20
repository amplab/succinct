package edu.berkeley.cs.succinct;

import junit.framework.TestCase;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class SuccinctIndexedBufferTest extends TestCase {

    private String testFileRaw = this.getClass().getResource("/test_file").getFile();
    private String testFileSuccinct = this.getClass().getResource("/test_file").getFile() + ".succinct";
    private SuccinctIndexedBuffer sIBuf;
    private long[] offsets;
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
        ArrayList<Long> positions = new ArrayList<Long>();
        positions.add(0L);
        for(int i = 0; i < fileData.length; i++) {
            if(fileData[i] == '\n') {
                positions.add(Long.valueOf(i + 1));
            }
        }
        offsets = new long[positions.size()];
        for(int i = 0; i < offsets.length; i++) {
            offsets[i] = positions.get(i);
        }
        sIBuf = new SuccinctIndexedBuffer(fileData, offsets);
    }

    /**
     * Test method: byte getRecordDelim()
     *
     * @throws Exception
     */
    public void testGetRecordDelim() throws Exception {
        System.out.println("getRecordDelim");
        
        assertEquals(SuccinctIndexedBuffer.getRecordDelim(), '\n');
    }

    /**
     * Test method: Long[] recordSearchOffsets(byte[] query)
     *
     * @throws Exception
     */
    public void testRecordSearchOffsets() throws Exception {
        System.out.println("recordSearchOffsets");

        Long[] searchOffsets = sIBuf.recordSearchOffsets("int".getBytes());
        for(int i = 0; i < searchOffsets.length; i++) {
            byte[] buf = sIBuf.extractUntil(searchOffsets[i].intValue(), (byte)'\n');
            assertTrue(new String(buf).contains("int"));
        }
    }

    /**
     * Test method: byte[][] recordSearch(byte[] query)
     *
     * @throws Exception
     */
    public void testRecordSearch() throws Exception {
        System.out.println("recordSearch");

        byte[][] records = sIBuf.recordSearch("int".getBytes());
        for(int i = 0; i < records.length; i++) {
            assertTrue(new String(records[i]).contains("int"));
        }
    }

    /**
     * Test method: long recordCount(byte[] query)
     *  
     * @throws Exception
     */
    public void testRecordCount() throws Exception {
        System.out.println("recordCount");

        long count = sIBuf.recordCount("int".getBytes());
        assertEquals(count, 28L);
    }

    /**
     * Test method: byte[][] extractRecords(int offset, int len)
     *  
     * @throws Exception
     */
    public void testExtractRecords() throws Exception {
        System.out.println("extractRecords");

        byte[][] records = sIBuf.extractRecords(0, 5);
        for(int i = 0; i < records.length; i++) {
            for(int j = 0; j < records[i].length; j++) {
                assertEquals(records[i][j], fileData[((int) (offsets[i] + j))]);
            }
        }
    }

    /**
     * Test method: byte[][] recordSearchRegex(byte[] query)
     *
     * @throws Exception
     */
    public void testRegexSearchRecords() throws Exception {
        System.out.println("regexSearchRecords");

        // TODO: Add more tests
        byte[][] records = sIBuf.recordSearchRegex("int");
        for(int i = 0; i < records.length; i++) {
            assertTrue(new String(records[i]).contains("int"));
        }
    }

    /**
     * Test method: byte[][] recordRangeSearch(byte[] queryBegin, byte[] queryEnd)
     *
     * @throws Exception
     */
    public void testRecordRangeSearch() throws Exception {
        System.out.println("recordRangeSearch");

        byte[][] records = sIBuf.recordRangeSearch("aa".getBytes(), "ac".getBytes());
        for(int i = 0; i < records.length; i++) {
            String currentRecord = new String(records[i]);
            assertTrue(currentRecord.contains("aa") || currentRecord.contains("ab") || currentRecord.contains("ac"));
        }
    }

    /**
     * Test method: byte[][] multiSearchUnion(byte[][] queries)
     *
     * @throws Exception
     */
    public void testMultiSearchUnion() throws Exception {
        System.out.println("multiSearchUnion");

        byte[][] queries = new byte[3][];
        queries[0] = "int".getBytes();
        queries[1] = "if".getBytes();
        queries[2] = "include".getBytes();

        byte[][] records = sIBuf.multiSearchUnion(queries);
        for(int i = 0; i < records.length; i++) {
            String currentRecord = new String(records[i]);
            assertTrue(currentRecord.contains("int") || currentRecord.contains("if") || currentRecord.contains("include"));
        }
    }

    /**
     * Test method: byte[][] multiSearchIntersect(byte[][] queries)
     *
     * @throws Exception
     */
    public void testMultiSearchIntersect() throws Exception {
        System.out.println("multiSearchIntersect");

        byte[][] queries = new byte[3][];
        queries[0] = "//".getBytes();
        queries[1] = "Creates".getBytes();
        queries[2] = "of".getBytes();

        byte[][] records = sIBuf.multiSearchIntersect(queries);
        for(int i = 0; i < records.length; i++) {
            String currentRecord = new String(records[i]);
            assertTrue(currentRecord.contains("//") && currentRecord.contains("Creates") && currentRecord.contains("of"));
        }
    }

    /**
     * Test method: byte[][] multiSearch(Pair<QueryType, byte[][]>[] queries)
     *
     * @throws Exception
     */
    public void testMultiSearch() throws Exception {
        System.out.println("multiSearch");

        SuccinctIndexedBuffer.QueryType[] queryTypes = new SuccinctIndexedBuffer.QueryType[2];
        byte[][][] queries = new byte[2][][];
        queryTypes[0] = SuccinctIndexedBuffer.QueryType.RangeSearch;
        queries[0] = new byte[][]{"/*".getBytes(), "//".getBytes()};
        queryTypes[1] = SuccinctIndexedBuffer.QueryType.Search;
        queries[1] = new byte[][]{"Build".getBytes()};

        byte[][] records = sIBuf.multiSearch(queryTypes, queries);
        for(int i = 0; i < records.length; i++) {
            String currentRecord = new String(records[i]);
            assertTrue((currentRecord.contains("/*") || currentRecord.contains("//")) && currentRecord.contains("Build"));
        }
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
        oos.writeObject(sIBuf);
        oos.close();

        // Deserialize data
        FileInputStream fIn = new FileInputStream(testFileSuccinct);
        ObjectInputStream ois = new ObjectInputStream(fIn);
        SuccinctIndexedBuffer sIBufRead = (SuccinctIndexedBuffer) ois.readObject();
        ois.close();

        assertNotNull(sIBufRead);
        assertEquals(sIBufRead.getOriginalSize(), sIBuf.getOriginalSize());
        assertTrue(Arrays.equals(sIBufRead.extract(0, sIBufRead.getOriginalSize()),
                                sIBuf.extract(0, sIBuf.getOriginalSize())));
    }
}
