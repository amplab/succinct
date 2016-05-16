package edu.berkeley.cs.succinct;

import junit.framework.TestCase;

import java.util.Iterator;
import java.util.Random;

abstract public class SuccinctIndexedFileTest extends TestCase {

  protected SuccinctIndexedFile sTFile;
  protected int[] offsets;
  protected byte[] fileData;

  /**
   * Test method: int getRecordOffset(int recordId)
   *
   * @throws Exception
   */
  public void testGetRecordOffset() throws Exception {

    for (int i = 0; i < offsets.length; i++) {
      assertEquals(offsets[i], sTFile.getRecordOffset(i));
    }
  }

  /**
   * Test method: byte[] getRecordBytes(int recordId)
   *
   * @throws Exception
   */
  public void testGetRecord() throws Exception {

    for (int i = 0; i < offsets.length; i++) {
      byte[] rec = sTFile.getRecordBytes(i);
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
        byte[] recordData = sTFile.extractRecordBytes(i, offset, length);
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

    Integer[] recordSearchIds1 = sTFile.recordSearchIds("int".getBytes());
    assertEquals(28, recordSearchIds1.length);
    for (Integer recordId : recordSearchIds1) {
      byte[] buf = sTFile.getRecordBytes(recordId);
      assertTrue(new String(buf).contains("int"));
    }

    Integer[] recordSearchIds2 = sTFile.recordSearchIds("include".getBytes());
    assertEquals(9, recordSearchIds2.length);
    for (Integer recordId : recordSearchIds2) {
      byte[] buf = sTFile.getRecordBytes(recordId);
      assertTrue(new String(buf).contains("include"));
    }

    Integer[] recordSearchIds3 = sTFile.recordSearchIds("random".getBytes());
    assertEquals(0, recordSearchIds3.length);

    Integer[] recordSearchIds4 = sTFile.recordSearchIds("random int".getBytes());
    assertEquals(0, recordSearchIds4.length);
  }

  /**
   * Test method: Iterator<Integer> recordSearchIdIterator(byte[] query)
   *
   * @throws Exception
   */
  public void testRecordSearchIdIterator() throws Exception {

    Iterator<Integer> recordSearchIds1 = sTFile.recordSearchIdIterator("int".getBytes());
    int count1 = 0;
    while (recordSearchIds1.hasNext()) {
      Integer recordId = recordSearchIds1.next();
      byte[] buf = sTFile.getRecordBytes(recordId);
      assertTrue(new String(buf).contains("int"));
      count1++;
    }
    assertEquals(28, count1);

    Iterator<Integer> recordSearchIds2 = sTFile.recordSearchIdIterator("include".getBytes());
    int count2 = 0;
    while (recordSearchIds2.hasNext()) {
      Integer recordId = recordSearchIds2.next();
      byte[] buf = sTFile.getRecordBytes(recordId);
      assertTrue(new String(buf).contains("include"));
      count2++;
    }
    assertEquals(9, count2);

    Iterator<Integer> recordSearchIds3 = sTFile.recordSearchIdIterator("random".getBytes());
    assertFalse(recordSearchIds3.hasNext());

    Iterator<Integer> recordSearchIds4 = sTFile.recordSearchIdIterator("random int".getBytes());
    assertFalse(recordSearchIds4.hasNext());
  }

  /**
   * Test method: Integer[] recordSearchRegexIds(byte[] query)
   *
   * @throws Exception
   */
  public void testRegexSearchIds() throws Exception {

    // TODO: Add more tests
    Integer[] recordsIds = sTFile.recordSearchRegexIds("int");
    for (Integer recordId: recordsIds) {
      assertTrue(new String(sTFile.getRecordBytes(recordId)).contains("int"));
    }
  }
}
