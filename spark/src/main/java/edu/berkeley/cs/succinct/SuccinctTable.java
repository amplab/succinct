package edu.berkeley.cs.succinct;

public interface SuccinctTable extends SuccinctIndexedFile {

  /**
   * Defines the types of search queries that SuccinctIndexedBuffer can handle in a recordMultiSearchIds.
   */
  enum QueryType {
    Search,
    RangeSearch
  }

  /**
   * Perform multiple searches with different query types and return the intersection of the results.
   *
   * @param queryTypes The QueryType corresponding to each query
   * @param queries    The actual query parameters associated with each query
   * @return The record ids matching the multi-search queries.
   */
  Integer[] recordMultiSearchIds(QueryType[] queryTypes, byte[][][] queries);
}
