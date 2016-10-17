package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.util.Source;
import edu.berkeley.cs.succinct.util.container.Range;

import java.util.Comparator;
import java.util.Iterator;

public interface SuccinctIndexedFile extends SuccinctFile {

  /**
   * Search for offset corresponding to a position in the input.
   *
   * @param pos Position in the input
   * @return Offset corresponding to the position.
   */
  int offsetToRecordId(int pos);

  /**
   * Get the number of records.
   *
   * @return The number of records.
   */
  int getNumRecords();

  /**
   * Get the offset for a given recordId
   *
   * @param recordId The record id.
   * @return The corresponding offset.
   */
  int getRecordOffset(int recordId);

  /**
   * Get the record for a given recordId.
   *
   * @param recordId The record id.
   * @return The corresponding record.
   */
  byte[] getRecordBytes(int recordId);

  /**
   * Get random access into record.
   *
   * @param recordId The record id.
   * @param offset   Offset into record.
   * @param length   Number of bytes to fetch.
   * @return The extracted data.
   */
  byte[] extractRecordBytes(int recordId, int offset, int length);

  /**
   * Get the record for a given recordId.
   *
   * @param recordId The record id.
   * @return The corresponding record.
   */
  String getRecord(int recordId);

  /**
   * Get random access into record.
   *
   * @param recordId The record id.
   * @param offset   Offset into record.
   * @param length   Number of bytes to fetch.
   * @return The extracted data.
   */
  String extractRecord(int recordId, int offset, int length);

  /**
   * Search for an input query and return ids of all matching records.
   *
   * @param query Input query.
   * @return Ids of all matching records.
   */
  Integer[] recordSearchIds(Source query);

  /**
   * Search for an input query and return ids of all matching records.
   *
   * @param query Input query.
   * @return Ids of all matching records.
   */
  Integer[] recordSearchIds(byte[] query);

  /**
   * Search for an input query and return ids of all matching records.
   *
   * @param query Input query.
   * @return Ids of all matching records.
   */
  Integer[] recordSearchIds(char[] query);

  /**
   * Search for an input query and return an iterator over ids of all matching records.
   *
   * @param query Input query.
   * @return Iterator over ids of all matching records
   */
  Iterator<Integer> recordSearchIdIterator(Source query);

  /**
   * Search for an input query and return an iterator over ids of all matching records.
   *
   * @param query Input query.
   * @return Iterator over ids of all matching records
   */
  Iterator<Integer> recordSearchIdIterator(byte[] query);

  /**
   * Search for an input query and return an iterator over ids of all matching records.
   *
   * @param query Input query.
   * @return Iterator over ids of all matching records
   */
  Iterator<Integer> recordSearchIdIterator(char[] query);

  /**
   * Search for all records that contain a particular regular expression.
   *
   * @param query The regular expression (UTF-8 encoded).
   * @return The record ids corresponding to records that contain the regular search expression.
   * @throws RegExParsingException
   */
  Integer[] recordSearchRegexIds(String query) throws RegExParsingException;

  /**
   * Comparator for range objects based on the size of the range.
   */
  class RangeSizeComparator implements Comparator<Range> {
    @Override public int compare(Range r1, Range r2) {
      return (int) ((r1.second - r1.first) - (r2.second - r2.first));
    }
  }

}
