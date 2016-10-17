package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.util.Source;
import junit.framework.TestCase;

import java.util.Iterator;
import java.util.Random;

import static edu.berkeley.cs.succinct.TestUtils.stringRecordCount;

abstract public class SuccinctIndexedFileTest extends TestCase {

  protected SuccinctIndexedFile sIFile;
  protected int[] offsets;
  protected Source fileData;

  abstract public String getQueryString(int i);

  abstract public int numQueryStrings();

  abstract public String getData();

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
   * Test method: byte[] getRecordBytes(int recordId)
   *
   * @throws Exception
   */
  public void testGetRecord() throws Exception {

    for (int i = 0; i < offsets.length; i++) {
      String rec = sIFile.getRecord(i);
      for (int j = 0; j < rec.length(); j++) {
        assertEquals(fileData.get(offsets[i] + j), rec.charAt(j));
      }
    }
  }

  /**
   * Test method: byte[] getAccess(int recordId, int offset, int length)
   *
   * @throws Exception
   */
  public void testExtractRecord() throws Exception {

    for (int i = 0; i < offsets.length - 1; i++) {
      int recordOffset = offsets[i];
      int recordLength = offsets[i + 1] - offsets[i];
      if (recordLength > 0) {
        int offset = (new Random()).nextInt(recordLength);
        int length = (new Random()).nextInt(recordLength - offset);
        String recordData = sIFile.extractRecord(i, offset, length);
        assertEquals(length, recordData.length());
        for (int j = 0; j < recordData.length(); j++) {
          assertEquals(fileData.get(recordOffset + offset + j), recordData.charAt(j));
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

    for (int i = 0; i < numQueryStrings(); i++) {
      String query = getQueryString(i);
      Integer[] recordSearchIds = sIFile.recordSearchIds(query.toCharArray());
      assertEquals(stringRecordCount(getData(), offsets, query), recordSearchIds.length);
      for (Integer recordId : recordSearchIds) {
        String buf = sIFile.getRecord(recordId);
        assertTrue(buf.contains(query));
      }
    }
  }



  /**
   * Test method: Iterator<Integer> recordSearchIdIterator(byte[] query)
   *
   * @throws Exception
   */
  public void testRecordSearchIdIterator() throws Exception {

    for (int i = 0; i < numQueryStrings(); i++) {
      String query = getQueryString(i);
      Iterator<Integer> recordSearchIds = sIFile.recordSearchIdIterator(query.toCharArray());
      int count = 0;
      while (recordSearchIds.hasNext()) {
        Integer recordId = recordSearchIds.next();
        String buf = sIFile.getRecord(recordId);
        assertTrue(buf.contains(query));
        count++;
      }
      assertEquals(stringRecordCount(getData(), offsets, query), count);
    }
  }

  /**
   * Test method: Integer[] recordSearchRegexIds(byte[] query)
   *
   * @throws Exception
   */
  public void testRegexSearchIds() throws Exception {

    // TODO: Add more tests
    Integer[] recordsIds = sIFile.recordSearchRegexIds("int");
    for (Integer recordId : recordsIds) {
      assertTrue(new String(sIFile.getRecordBytes(recordId)).contains("int"));
    }
  }
}
