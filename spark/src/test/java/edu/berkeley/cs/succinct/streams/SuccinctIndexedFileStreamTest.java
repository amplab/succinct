package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

public class SuccinctIndexedFileStreamTest extends TestCase {
  private String testFileRaw = this.getClass().getResource("/raw.dat").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/raw.dat").getFile() + ".idx.succinct";
  private SuccinctIndexedFileStream sStream;
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
    DataInputStream dis = new DataInputStream(new FileInputStream(inputFile));
    dis.readFully(fileData);
    ArrayList<Integer> positions = new ArrayList<Integer>();
    positions.add(0);
    for (int i = 0; i < fileData.length; i++) {
      if (fileData[i] == '\n') {
        positions.add(i + 1);
      }
    }
    offsets = new int[positions.size()];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = positions.get(i);
    }
    SuccinctIndexedFileBuffer sBuf = new SuccinctIndexedFileBuffer(fileData, offsets);
    sBuf.writeToFile(testFileSuccinct);

    sStream = new SuccinctIndexedFileStream(new Path(testFileSuccinct));

  }

  /**
   * Test method: Long[] recordSearchOffsets(byte[] query)
   *
   * @throws Exception
   */
  public void testRecordSearchIds() throws Exception {
    System.out.println("recordSearchOffsets");

    Integer[] recordSearchIds = sStream.recordSearchIds("int".getBytes());
    for (Integer recordSearchId: recordSearchIds) {
      byte[] buf = sStream.getRecord(recordSearchId);
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

    byte[][] records = sStream.recordSearch("int".getBytes());
    for (int i = 0; i < records.length; i++) {
      assertTrue(new String(records[i]).contains("int"));
    }
  }

  /**
   * Test method: byte[][] recordSearchRegex(byte[] query)
   *
   * @throws Exception
   */
  public void testRegexSearchRecords() throws Exception {
    System.out.println("regexSearch");

    // TODO: Add more tests
    byte[][] records = sStream.recordSearchRegex("int");
    for (int i = 0; i < records.length; i++) {
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

    byte[][] records = sStream.recordRangeSearch("aa".getBytes(), "ac".getBytes());
    for (int i = 0; i < records.length; i++) {
      String currentRecord = new String(records[i]);
      assertTrue(currentRecord.contains("aa") || currentRecord.contains("ab") || currentRecord
        .contains("ac"));
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
    queries[0] = new byte[][] {"/*".getBytes(), "//".getBytes()};
    queryTypes[1] = SuccinctIndexedFileBuffer.QueryType.Search;
    queries[1] = new byte[][] {"Build".getBytes()};

    byte[][] records = sStream.multiSearch(queryTypes, queries);
    for (int i = 0; i < records.length; i++) {
      String currentRecord = new String(records[i]);
      assertTrue((currentRecord.contains("/*") || currentRecord.contains("//")) && currentRecord
        .contains("Build"));
    }
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
