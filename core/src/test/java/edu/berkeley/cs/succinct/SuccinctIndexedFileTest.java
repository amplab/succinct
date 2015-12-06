package edu.berkeley.cs.succinct;

import junit.framework.TestCase;

import java.util.Iterator;
import java.util.Random;

abstract public class SuccinctIndexedFileTest extends TestCase {

  protected SuccinctIndexedFile sIFile;
  protected int[] offsets;
  protected byte[] fileData;

  /**
   * Test method: int getRecordOffset(int recordId)
   *
   * @throws Exception
   */
  public void testGetRecordOffset() throws Exception {

    for (int i = 0; i < offsets.length; i++) {
      assertEquals(offsets[i], sIFile.getRecordOffset(i));
    }
  }

  /**
   * Test method: byte[] getRecord(int recordId)
   *
   * @throws Exception
   */
  public void testGetRecord() throws Exception {

    for (int i = 0; i < offsets.length; i++) {
      byte[] rec = sIFile.getRecord(i);
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

    for (int i = 0; i < offsets.length - 1; i++) {
      int recordOffset = offsets[i];
      int recordLength = offsets[i + 1] - offsets[i];
      if (recordLength > 0) {
        int offset = (new Random()).nextInt(recordLength);
        int length = (new Random()).nextInt(recordLength - offset);
        byte[] recordData = sIFile.accessRecord(i, offset, length);
        assertEquals(length, recordData.length);
        for (int j = 0; j < recordData.length; j++) {
          assertEquals(fileData[recordOffset + offset + j], recordData[j]);
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

    Integer[] recordSearchIds1 = sIFile.recordSearchIds("int".getBytes());
    assertEquals(28, recordSearchIds1.length);
    for (Integer recordId : recordSearchIds1) {
      byte[] buf = sIFile.getRecord(recordId);
      assertTrue(new String(buf).contains("int"));
    }

    Integer[] recordSearchIds2 = sIFile.recordSearchIds("include".getBytes());
    assertEquals(9, recordSearchIds2.length);
    for (Integer recordId : recordSearchIds2) {
      byte[] buf = sIFile.getRecord(recordId);
      assertTrue(new String(buf).contains("include"));
    }

    Integer[] recordSearchIds3 = sIFile.recordSearchIds("random".getBytes());
    assertEquals(0, recordSearchIds3.length);

    Integer[] recordSearchIds4 = sIFile.recordSearchIds("random int".getBytes());
    assertEquals(0, recordSearchIds4.length);
  }

  /**
   * Test method: Iterator<Integer> recordSearchIdIterator(byte[] query)
   *
   * @throws Exception
   */
  public void testRecordSearchIdIterator() throws Exception {

    Iterator<Integer> recordSearchIds1 = sIFile.recordSearchIdIterator("int".getBytes());
    int count1 = 0;
    while (recordSearchIds1.hasNext()) {
      Integer recordId = recordSearchIds1.next();
      byte[] buf = sIFile.getRecord(recordId);
      assertTrue(new String(buf).contains("int"));
      count1++;
    }
    assertEquals(28, count1);

    Iterator<Integer> recordSearchIds2 = sIFile.recordSearchIdIterator("include".getBytes());
    int count2 = 0;
    while (recordSearchIds2.hasNext()) {
      Integer recordId = recordSearchIds2.next();
      byte[] buf = sIFile.getRecord(recordId);
      assertTrue(new String(buf).contains("include"));
      count2++;
    }
    assertEquals(9, count2);

    Iterator<Integer> recordSearchIds3 = sIFile.recordSearchIdIterator("random".getBytes());
    assertFalse(recordSearchIds3.hasNext());

    Iterator<Integer> recordSearchIds4 = sIFile.recordSearchIdIterator("random int".getBytes());
    assertFalse(recordSearchIds4.hasNext());


  }

  /**
   * Test method: Integer[] recordSearchRegexIds(byte[] query)
   *
   * @throws Exception
   */
  public void testRegexSearchIds() throws Exception {

    // TODO: Add more tests
    Integer[] recordsIds = sIFile.recordSearchRegexIds("int");
    for (Integer recordId: recordsIds) {
      assertTrue(new String(sIFile.getRecord(recordId)).contains("int"));
    }
  }

  /**
   * Test method: Integer[] recordMultiSearchIds(Pair<QueryType, byte[][]>[] queries)
   *
   * @throws Exception
   */
  public void testMultiSearchIds() throws Exception {

    SuccinctIndexedFile.QueryType[] queryTypes = new SuccinctIndexedFile.QueryType[2];
    byte[][][] queries = new byte[2][][];
    queryTypes[0] = SuccinctIndexedFile.QueryType.RangeSearch;
    queries[0] = new byte[][] {"/*".getBytes(), "//".getBytes()};
    queryTypes[1] = SuccinctIndexedFile.QueryType.Search;
    queries[1] = new byte[][] {"Build".getBytes()};

    Integer[] recordIds = sIFile.recordMultiSearchIds(queryTypes, queries);
    for (Integer recordId : recordIds) {
      String currentRecord = new String(sIFile.getRecord(recordId));
      assertTrue((currentRecord.contains("/*") || currentRecord.contains("//")) && currentRecord
        .contains("Build"));
    }
  }
}
