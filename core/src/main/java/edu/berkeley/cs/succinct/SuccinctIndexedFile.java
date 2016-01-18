package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.util.container.Range;

import java.util.Comparator;
import java.util.Iterator;

public interface SuccinctIndexedFile extends SuccinctFile {

  /**
   * Get the size of the Succinct compressed file.
   *
   * @return The size of the Succinct compressed file.
   */
  int getSuccinctIndexedFileSize();

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
  byte[] getRecord(int recordId);

  /**
   * Get random access into record.
   *
   * @param recordId The record id.
   * @param offset Offset into record.
   * @param length Number of bytes to fetch.
   * @return The extracted data.
   */
  byte[] accessRecord(int recordId, int offset, int length);

  /**
   * Search for an input query and return ids of all matching records.
   *
   * @param query Input query.
   * @return Ids of all matching records.
   */
  Integer[] recordSearchIds(byte[] query);

  /**
   * Search for an input query and return an iterator over ids of all matching records.
   *
   * @param query Input query.
   * @return Iterator over ids of all matching records
   */
  Iterator<Integer> recordSearchIdIterator(byte[] query);

  /**
   * Search for all records that contain a particular regular expression.
   *
   * @param query The regular expression (UTF-8 encoded).
   * @return The record ids corresponding to records that contain the regular search expression.
   * @throws RegExParsingException
   */
  Integer[] recordSearchRegexIds(String query) throws RegExParsingException;

  /**
   * Perform multiple searches with different query types and return the intersection of the results.
   *
   * @param queryTypes The QueryType corresponding to each query
   * @param queries    The actual query parameters associated with each query
   * @return The record ids matching the multi-search queries.
   */
  Integer[] recordMultiSearchIds(QueryType[] queryTypes, byte[][][] queries);

  /**
   * Defines the types of search queries that SuccinctIndexedBuffer can handle in a recordMultiSearchIds.
   */
  enum QueryType {
    Search,
    RangeSearch
  }

  /**
   * Comparator for range objects based on the size of the range.
   */
  class RangeSizeComparator implements Comparator<Range> {
    @Override public int compare(Range r1, Range r2) {
      return (int) ((r1.second - r1.first) - (r2.second - r2.first));
    }
  }

}
