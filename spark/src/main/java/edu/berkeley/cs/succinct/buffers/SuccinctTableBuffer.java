package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctTable;
import edu.berkeley.cs.succinct.util.container.Range;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.*;

public class SuccinctTableBuffer extends SuccinctIndexedFileBuffer implements SuccinctTable {

  /**
   * Constructor to initialize SuccinctIndexedBuffer from input byte array and offsets corresponding to records.
   *
   * @param input   The input byte array.
   * @param offsets Offsets corresponding to records.
   */
  public SuccinctTableBuffer(byte[] input, int[] offsets) {
    super(input, offsets);
  }

  /**
   * Constructor to load the data from persisted Succinct data-structures.
   *
   * @param path        Path to load data from.
   * @param storageMode Mode in which data is stored (In-memory or Memory-mapped)
   */
  public SuccinctTableBuffer(String path, StorageMode storageMode) {
    super(path, storageMode);
  }

  /**
   * Constructor to load the data from a DataInputStream.
   *
   * @param is Input stream to load the data from
   */
  public SuccinctTableBuffer(DataInputStream is) {
    super(is);
  }

  /**
   * Constructor to load the data from a ByteBuffer.
   *
   * @param buf Input buffer to load the data from
   */
  public SuccinctTableBuffer(ByteBuffer buf) {
    super(buf);
  }

  /**
   * Perform multiple searches with different query types and return the intersection of the results.
   *
   * @param queryTypes The QueryType corresponding to each query
   * @param queries    The actual query parameters associated with each query
   * @return The records matching the multi-search queries.
   */
  @Override public Integer[] recordMultiSearchIds(SuccinctTable.QueryType[] queryTypes, byte[][][] queries) {
    assert (queryTypes.length == queries.length);
    Set<Integer> recordIds = new HashSet<>();

    if (queries.length == 0) {
      throw new IllegalArgumentException("recordMultiSearchIds called with empty queries");
    }

    // Get all ranges
    ArrayList<Range> ranges = new ArrayList<>();
    for (int qid = 0; qid < queries.length; qid++) {
      Range range;

      switch (queryTypes[qid]) {
        case Search: {
          range = bwdSearch(queries[qid][0]);
          break;
        }
        case RangeSearch: {
          byte[] queryBegin = queries[qid][0];
          byte[] queryEnd = queries[qid][1];
          range = rangeSearch(queryBegin, queryEnd);
          break;
        }
        default: {
          throw new UnsupportedOperationException("Unsupported QueryType");
        }
      }

      if (range.second - range.first + 1 > 0) {
        ranges.add(range);
      } else {
        return new Integer[0];
      }
    }

    Collections.sort(ranges, new RangeSizeComparator());

    // Populate the set of offsets corresponding to the first range
    Range firstRange = ranges.get(0);
    Map<Integer, Integer> counts = new HashMap<>();
    {
      long sp = firstRange.first, ep = firstRange.second;
      for (long i = 0; i < ep - sp + 1; i++) {
        long saVal = lookupSA(sp + i);
        int recordId = offsetToRecordId((int) saVal);
        recordIds.add(recordId);
        counts.put(recordId, 1);
      }
    }

    ranges.remove(firstRange);
    for (Range range : ranges) {
      long sp = range.first, ep = range.second;
      for (long i = 0; i < ep - sp + 1; i++) {
        long saVal = lookupSA(sp + i);
        int recordId = offsetToRecordId((int) saVal);
        if (recordIds.contains(recordId)) {
          counts.put(recordId, counts.get(recordId) + 1);
        }
      }
    }

    return recordIds.toArray(new Integer[recordIds.size()]);
  }
}
