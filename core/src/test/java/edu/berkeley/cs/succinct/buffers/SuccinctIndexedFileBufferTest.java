package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import junit.framework.TestCase;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class SuccinctIndexedFileBufferTest extends TestCase {

    private String testFileRaw = this.getClass().getResource("/test_file").getFile();
    private String testFileSuccinct = this.getClass().getResource("/test_file").getFile() + ".idx.succinct";
    private String testFileSuccinctMin = this.getClass().getResource("/test_file").getFile() + ".idx.min.succinct";
    private SuccinctIndexedFileBuffer sIBuf;
    private int[] offsets;
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
        ArrayList<Integer> positions = new ArrayList<Integer>();
        positions.add(0);
        for(int i = 0; i < fileData.length; i++) {
            if(fileData[i] == '\n') {
                positions.add(i + 1);
            }
        }
        offsets = new int[positions.size()];
        for(int i = 0; i < offsets.length; i++) {
            offsets[i] = positions.get(i);
        }
        sIBuf = new SuccinctIndexedFileBuffer(fileData, offsets);
    }

    /**
     * Test method: Long[] recordSearchOffsets(byte[] query)
     *
     * @throws Exception
     */
    public void testRecordSearchOffsets() throws Exception {
        System.out.println("recordSearchOffsets");

        Integer[] searchOffsets = sIBuf.recordSearchOffsets("int".getBytes());
        for(int i = 0; i < searchOffsets.length; i++) {
            byte[] buf = sIBuf.extractUntil(searchOffsets[i], (byte)'\n');
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
     * Test method: byte[][] multiSearch(Pair<QueryType, byte[][]>[] queries)
     *
     * @throws Exception
     */
    public void testMultiSearch() throws Exception {
        System.out.println("multiSearch");

        SuccinctIndexedFileBuffer.QueryType[] queryTypes = new SuccinctIndexedFileBuffer.QueryType[2];
        byte[][][] queries = new byte[2][][];
        queryTypes[0] = SuccinctIndexedFileBuffer.QueryType.RangeSearch;
        queries[0] = new byte[][]{"/*".getBytes(), "//".getBytes()};
        queryTypes[1] = SuccinctIndexedFileBuffer.QueryType.Search;
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
        SuccinctIndexedFileBuffer sIBufRead = (SuccinctIndexedFileBuffer) ois.readObject();
        ois.close();

        assertNotNull(sIBufRead);
        assertEquals(sIBufRead.getOriginalSize(), sIBuf.getOriginalSize());
        assertTrue(Arrays.equals(sIBufRead.extract(0, sIBufRead.getOriginalSize()),
                                sIBuf.extract(0, sIBuf.getOriginalSize())));
    }

    /**
     * Test method: void writeToFile(String path)
     * Test method: void memoryMap(String path)
     *
     * @throws Exception
     */
    public void testMemoryMap() throws Exception {
        System.out.println("memoryMap");

        sIBuf.writeToFile(testFileSuccinctMin);
        SuccinctIndexedFileBuffer sIBufRead = new SuccinctIndexedFileBuffer(testFileSuccinctMin, StorageMode.MEMORY_MAPPED);

        assertNotNull(sIBufRead);
        assertEquals(sIBufRead.getOriginalSize(), sIBuf.getOriginalSize());
        assertTrue(Arrays.equals(sIBufRead.extract(0, sIBufRead.getOriginalSize()),
                sIBuf.extract(0, sIBuf.getOriginalSize())));
        assertTrue(Arrays.equals(sIBufRead.offsets, sIBuf.offsets));
    }

    /**
     * Test method: void writeToFile(String path)
     * Test method: void readFromFile(String path)
     *
     * @throws Exception
     */
    public void testReadFromFile() throws Exception {
        System.out.println("readFromFile");

        sIBuf.writeToFile(testFileSuccinctMin);
        SuccinctIndexedFileBuffer sIBufRead = new SuccinctIndexedFileBuffer(testFileSuccinctMin, StorageMode.MEMORY_ONLY);

        assertNotNull(sIBufRead);
        assertEquals(sIBufRead.getOriginalSize(), sIBuf.getOriginalSize());
        assertTrue(Arrays.equals(sIBufRead.extract(0, sIBufRead.getOriginalSize()),
                sIBuf.extract(0, sIBuf.getOriginalSize())));
        assertTrue(Arrays.equals(sIBufRead.offsets, sIBuf.offsets));
    }
}
