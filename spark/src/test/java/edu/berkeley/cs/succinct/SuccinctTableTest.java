package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.util.Source;
import junit.framework.TestCase;

public abstract class SuccinctTableTest extends TestCase {

  protected SuccinctTable sTable;
  protected int[] offsets;
  protected Source fileData;
  protected SuccinctTable.QueryType[] queryTypes;
  protected byte[][][] queries;

  /**
   * Test method: Integer[] recordMultiSearchIds(Pair<QueryType, byte[][]>[] queries)
   *
   * @throws Exception
   */
  public void testMultiSearchIds() throws Exception {
    Integer[] recordIds = sTable.recordMultiSearchIds(queryTypes, queries);
    for (Integer recordId : recordIds) {
      String currentRecord = new String(sTable.getRecordBytes(recordId));
      assertTrue((currentRecord.contains("/*") || currentRecord.contains("//")) && currentRecord
        .contains("Build"));
    }
  }
}
