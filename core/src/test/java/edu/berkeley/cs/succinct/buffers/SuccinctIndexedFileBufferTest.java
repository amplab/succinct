package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import junit.framework.TestCase;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class SuccinctIndexedFileBufferTest extends TestCase {

  private String testFileRaw = this.getClass().getResource("/test_file").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/test_file").getFile() + ".idx.succinct";
  private String testFileSuccinctMin =
    this.getClass().getResource("/test_file").getFile() + ".idx.min.succinct";
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
    sIBuf = new SuccinctIndexedFileBuffer(fileData, offsets);
  }

  /**
   * Test method: byte[] getRecord(int recordId)
   *
   * @throws Exception
   */
  public void testGetRecord() throws Exception {
    System.out.println("getRecord");

    for (int i = 0; i < offsets.length; i++) {
      byte[] rec = sIBuf.getRecord(i);
      for (int j = 0; j < rec.length; j++) {
        assertEquals(fileData[offsets[i] + j], rec[j]);
      }
    }
  }

  /**
   * Test method: byte[] getAccess(int recordId, int offset, int length)
   *
   * @throws Exception
   */
  public void testAccessRecord() throws Exception {
    System.out.println("accessRecord");

    for (int i = 0; i < offsets.length - 1; i++) {
      if (offsets[i + 1] - offsets[i] > 10) {
        byte[] rec = sIBuf.accessRecord(i, 5, 5);
        assertEquals(rec.length, 5);
        for (int j = 0; j < rec.length; j++) {
          assertEquals(fileData[offsets[i] + j + 5], rec[j]);
        }
      }
    }
  }

  /**
   * Test method: Integer[] recordSearchIds(byte[] query)
   *
   * @throws Exception
   */
  public void testRecordSearchIds() throws Exception {
    System.out.println("recordSearchIds");

    Integer[] recordSearchIds = sIBuf.recordSearchIds("int".getBytes());
    for (Integer recordId : recordSearchIds) {
      byte[] buf = sIBuf.getRecord(recordId);
      assertTrue(new String(buf).contains("int"));
    }
    assertEquals(recordSearchIds.length, 28);
  }

  /**
   * Test method: Iterator<Integer> recordSearchIdIterator(byte[] query)
   *
   * @throws Exception
   */
  public void testRecordSearchIdIterator() throws Exception {
    System.out.println("recordSearchIdIterator");

    Iterator<Integer> recordSearchIds = sIBuf.recordSearchIdIterator("int".getBytes());
    int count = 0;
    while (recordSearchIds.hasNext()) {
      Integer recordId = recordSearchIds.next();
      byte[] buf = sIBuf.getRecord(recordId);
      assertTrue(new String(buf).contains("int"));
      count++;
    }
    assertEquals(count, 28);
  }

  /**
   * Test method: Integer[] recordSearchRegexIds(byte[] query)
   *
   * @throws Exception
   */
  public void testRegexSearchIds() throws Exception {
    System.out.println("regexSearchRecordIds");

    // TODO: Add more tests
    Integer[] recordsIds = sIBuf.recordSearchRegexIds("int");
    for (Integer recordId: recordsIds) {
      assertTrue(new String(sIBuf.getRecord(recordId)).contains("int"));
    }
  }

  /**
   * Test method: Integer[] recordMultiSearchIds(Pair<QueryType, byte[][]>[] queries)
   *
   * @throws Exception
   */
  public void testMultiSearchIds() throws Exception {
    System.out.println("recordMultiSearchIds");

    SuccinctIndexedFileBuffer.QueryType[] queryTypes = new SuccinctIndexedFileBuffer.QueryType[2];
    byte[][][] queries = new byte[2][][];
    queryTypes[0] = SuccinctIndexedFileBuffer.QueryType.RangeSearch;
    queries[0] = new byte[][] {"/*".getBytes(), "//".getBytes()};
    queryTypes[1] = SuccinctIndexedFileBuffer.QueryType.Search;
    queries[1] = new byte[][] {"Build".getBytes()};

    Integer[] recordIds = sIBuf.recordMultiSearchIds(queryTypes, queries);
    for (Integer recordId : recordIds) {
      String currentRecord = new String(sIBuf.getRecord(recordId));
      assertTrue((currentRecord.contains("/*") || currentRecord.contains("//")) && currentRecord
        .contains("Build"));
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
    SuccinctIndexedFileBuffer sIBufRead =
      new SuccinctIndexedFileBuffer(testFileSuccinctMin, StorageMode.MEMORY_MAPPED);

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
    SuccinctIndexedFileBuffer sIBufRead =
      new SuccinctIndexedFileBuffer(testFileSuccinctMin, StorageMode.MEMORY_ONLY);

    assertNotNull(sIBufRead);
    assertEquals(sIBufRead.getOriginalSize(), sIBuf.getOriginalSize());
    assertTrue(Arrays.equals(sIBufRead.extract(0, sIBufRead.getOriginalSize()),
      sIBuf.extract(0, sIBuf.getOriginalSize())));
    assertTrue(Arrays.equals(sIBufRead.offsets, sIBuf.offsets));
  }
}
