package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.util.Range;

import java.util.Comparator;

public interface SuccinctIndexedFile extends SuccinctFile {

    /**
     * Defines the types of search queries that SuccinctIndexedBuffer can handle in a multiSearch.
     */
    enum QueryType {
        Search,
        RangeSearch
    }

    /**
     * Comparator for range objects based on the size of the range.
     */
    class RangeSizeComparator implements Comparator<Range> {
        @Override
        public int compare(Range r1, Range r2) {
            return (int)((r1.second - r1.first) - (r2.second - r2.first));
        }
    }

    /**
     * Search for offset corresponding to a position in the input.
     *
     * @param pos Position in the input
     * @return Offset corresponding to the position.
     */
    int searchOffset(int pos);

    /**
     * Get the number of records.
     *
     * @return The number of records.
     */
    int getNumRecords();

    /**
     * Get the ith record.
     *
     * @param i The record index.
     * @return The corresponding record.
     */
    byte[] getRecord(int i);

    /**
     * Search for an input query and return offsets of all matching records.
     *
     * @param query Input query.
     * @return Offsets of all matching records.
     */
    Integer[] recordSearchOffsets(byte[] query);

    /**
     * Count of all records containing a particular query.
     *
     * @param query Input query.
     * @return Count of all records containing input query.
     */
    long recordCount(byte[] query);

    /**
     * Search for all records that contains the query.
     *
     * @param query Input query.
     * @return All records containing input query.
     */
    byte[][] recordSearch(byte[] query);

    /**
     * Performs a range search for all records that contains a substring between queryBegin and queryEnd.
     *
     * @param queryBegin The beginning of query range.
     * @param queryEnd The end of query range.
     * @return All records matching the query range.
     */
    byte[][] recordRangeSearch(byte[] queryBegin, byte[] queryEnd);

    /**
     * Search for all records that contain a particular regular expression.
     *
     * @param query The regular expression (UTF-8 encoded).
     * @return The records that contain the regular search expression.
     * @throws RegExParsingException
     */
    byte[][] recordSearchRegex(String query) throws RegExParsingException;

    /**
     * Perform multiple searches with different query types and return the intersection of the results.
     *
     * @param queryTypes The QueryType corresponding to each query
     * @param queries The actual query parameters associated with each query
     * @return The records matching the multi-search queries.
     */
    byte[][] multiSearch(QueryType[] queryTypes, byte[][][] queries);

    /**
     * Extract a part of all records.
     *
     * @param offset Offset into record.
     * @param length Length of part to be extracted.
     * @return Extracted data.
     */
    byte[][] extractRecords(int offset, int length);

}
