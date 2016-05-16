package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.buffers.SuccinctTableBuffer;
import junit.framework.TestCase;

public abstract class SuccinctTableTest extends TestCase {

  protected SuccinctTableBuffer sTable;
  protected int[] offsets;
  protected byte[] fileData;

  /**
   * Test method: Integer[] recordMultiSearchIds(Pair<QueryType, byte[][]>[] queries)
   *
   * @throws Exception
   */
  public void testMultiSearchIds() throws Exception {

    SuccinctTable.QueryType[] queryTypes = new SuccinctTable.QueryType[2];
    byte[][][] queries = new byte[2][][];
    queryTypes[0] = SuccinctTable.QueryType.RangeSearch;
    queries[0] = new byte[][] {"/*".getBytes(), "//".getBytes()};
    queryTypes[1] = SuccinctTable.QueryType.Search;
    queries[1] = new byte[][] {"Build".getBytes()};

    Integer[] recordIds = sTable.recordMultiSearchIds(queryTypes, queries);
    for (Integer recordId : recordIds) {
      String currentRecord = new String(sTable.getRecordBytes(recordId));
      assertTrue((currentRecord.contains("/*") || currentRecord.contains("//")) && currentRecord
        .contains("Build"));
    }
  }
}
