package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctIndexedFile;
import edu.berkeley.cs.succinct.SuccinctTable;
import edu.berkeley.cs.succinct.util.container.Range;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

public class SuccinctTableStream extends SuccinctIndexedFileStream implements SuccinctTable {

  /**
   * Constructor to map a file containing Succinct data structures via stream.
   *
   * @param filePath Path of the file.
   * @param conf     Configuration for the filesystem.
   * @throws IOException
   */
  public SuccinctTableStream(Path filePath, Configuration conf) throws IOException {
    super(filePath, conf);
  }

  /**
   * Constructor to map a file containing Succinct data structures via stream.
   *
   * @param filePath Path of the file.
   * @throws IOException
   */
  public SuccinctTableStream(Path filePath) throws IOException {
    super(filePath);
  }

  @Override public Integer[] recordMultiSearchIds(QueryType[] queryTypes, byte[][][] queries) {
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

    Collections.sort(ranges, new SuccinctIndexedFile.RangeSizeComparator());

    // Populate the set of recordIds corresponding to the first range
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
