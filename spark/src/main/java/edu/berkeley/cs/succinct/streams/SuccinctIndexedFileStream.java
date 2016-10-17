package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctIndexedFile;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.util.Source;
import edu.berkeley.cs.succinct.util.SuccinctConstants;
import edu.berkeley.cs.succinct.util.container.Range;
import edu.berkeley.cs.succinct.util.iterator.SearchIterator;
import edu.berkeley.cs.succinct.util.iterator.SearchRecordIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class SuccinctIndexedFileStream extends SuccinctFileStream implements SuccinctIndexedFile {

  protected transient int[] offsets;
  protected transient long endOfIndexedFileStream;

  /**
   * Constructor to map a file containing Succinct data structures via stream.
   *
   * @param filePath Path of the file.
   * @param conf     Configuration for the filesystem.
   * @throws IOException
   */
  public SuccinctIndexedFileStream(Path filePath, Configuration conf) throws IOException {
    super(filePath, conf);
    FSDataInputStream is = getStream(filePath);
    is.seek(endOfFileStream);
    int len = is.readInt();
    offsets = new int[len];
    for (int i = 0; i < len; i++) {
      offsets[i] = is.readInt();
    }
    endOfIndexedFileStream = is.getPos();
    is.close();
  }

  /**
   * Constructor to map a file containing Succinct data structures via stream.
   *
   * @param filePath Path of the file.
   * @throws IOException
   */
  public SuccinctIndexedFileStream(Path filePath) throws IOException {
    this(filePath, new Configuration());
  }

  @Override public int getCompressedSize() {
    return super.getCompressedSize() + (12 + offsets.length * SuccinctConstants.INT_SIZE_BYTES);
  }

  public int offsetToRecordId(int pos) {
    int sp = 0, ep = offsets.length - 1;
    int m;

    while (sp <= ep) {
      m = (sp + ep) / 2;
      if (offsets[m] == pos) {
        return m;
      } else if (pos < offsets[m]) {
        ep = m - 1;
      } else {
        sp = m + 1;
      }
    }

    return ep;
  }

  public int getNumRecords() {
    return offsets.length;
  }

  @Override public int getRecordOffset(int recordId) {
    if (recordId >= offsets.length || recordId < 0) {
      throw new ArrayIndexOutOfBoundsException("Record does not exist: recordId = " + recordId);
    }

    return offsets[recordId];
  }

  @Override public byte[] getRecordBytes(int recordId) {
    if (recordId >= offsets.length || recordId < 0) {
      throw new ArrayIndexOutOfBoundsException("Record does not exist: recordId = " + recordId);
    }
    int begOffset = offsets[recordId];
    int endOffset =
      (recordId == offsets.length - 1) ? getOriginalSize() - 1 : offsets[recordId + 1];
    int len = (endOffset - begOffset - 1);
    return extractBytes(begOffset, len);
  }

  @Override public byte[] extractRecordBytes(int recordId, int offset, int length) {
    if (recordId >= offsets.length || recordId < 0) {
      throw new ArrayIndexOutOfBoundsException("Record does not exist: recordId = " + recordId);
    }

    if (length == 0) {
      return new byte[0];
    }

    int begOffset = offsets[recordId] + offset;
    int nextRecordOffset =
      (recordId == offsets.length - 1) ? getOriginalSize() - 1 : offsets[recordId + 1];
    length = Math.min(nextRecordOffset - begOffset - 1, length);
    return extractBytes(begOffset, length);
  }

  /**
   * Get the record for a given recordId.
   *
   * @param recordId The record id.
   * @return The corresponding record.
   */
  @Override public String getRecord(int recordId) {
    if (recordId >= offsets.length || recordId < 0) {
      throw new ArrayIndexOutOfBoundsException("Record does not exist: recordId = " + recordId);
    }
    int begOffset = offsets[recordId];
    int endOffset =
      (recordId == offsets.length - 1) ? getOriginalSize() - 1 : offsets[recordId + 1];
    int len = (endOffset - begOffset - 1);
    return extract(begOffset, len);
  }

  /**
   * Get random access into record.
   *
   * @param recordId The record id.
   * @param offset   Offset into record.
   * @param length   Number of bytes to fetch.
   * @return The extracted data.
   */
  @Override public String extractRecord(int recordId, int offset, int length) {
    if (recordId >= offsets.length || recordId < 0) {
      throw new ArrayIndexOutOfBoundsException("Record does not exist: recordId = " + recordId);
    }

    if (length == 0) {
      return "";
    }

    int begOffset = offsets[recordId] + offset;
    int nextRecordOffset =
      (recordId == offsets.length - 1) ? getOriginalSize() - 1 : offsets[recordId + 1];
    length = Math.min(nextRecordOffset - begOffset - 1, length);
    return extract(begOffset, length);
  }

  /**
   * Search for an input query and return ids of all matching records.
   *
   * @param query Input query.
   * @return Ids of all matching records.
   */
  @Override public Integer[] recordSearchIds(Source query) {
    Set<Integer> results = new HashSet<>();
    Range range = bwdSearch(query);

    long sp = range.first, ep = range.second;
    if (ep - sp + 1 <= 0) {
      return new Integer[0];
    }

    for (long i = 0; i < ep - sp + 1; i++) {
      results.add(offsetToRecordId((int) lookupSA(sp + i)));
    }

    return results.toArray(new Integer[results.size()]);
  }

  /**
   * Search for an input query and return offsets of all matching records.
   *
   * @param query Input query.
   * @return Offsets of all matching records.
   */
  @Override public Integer[] recordSearchIds(final byte[] query) {
    return recordSearchIds(new Source() {
      @Override public int length() {
        return query.length;
      }

      @Override public int get(int i) {
        return query[i];
      }
    });
  }

  /**
   * Search for an input query and return ids of all matching records.
   *
   * @param query Input query.
   * @return Ids of all matching records.
   */
  @Override public Integer[] recordSearchIds(final char[] query) {
    return recordSearchIds(new Source() {
      @Override public int length() {
        return query.length;
      }

      @Override public int get(int i) {
        return query[i];
      }
    });
  }

  /**
   * Search for an input query and return an iterator over ids of all matching records.
   *
   * @param query Input query.
   * @return Iterator over ids of all matching records
   */
  @Override public Iterator<Integer> recordSearchIdIterator(Source query) {
    SearchIterator searchIterator = (SearchIterator) searchIterator(query);
    return new SearchRecordIterator(searchIterator, this);
  }

  /**
   * Search for an input query and return an iterator over ids of all matching records.
   *
   * @param query Input query.
   * @return Iterator over ids of all matching records
   */
  @Override public Iterator<Integer> recordSearchIdIterator(final byte[] query) {
    return recordSearchIdIterator(new Source() {
      @Override public int length() {
        return query.length;
      }

      @Override public int get(int i) {
        return query[i];
      }
    });
  }

  /**
   * Search for an input query and return an iterator over ids of all matching records.
   *
   * @param query Input query.
   * @return Iterator over ids of all matching records
   */
  @Override public Iterator<Integer> recordSearchIdIterator(final char[] query) {
    return recordSearchIdIterator(new Source() {
      @Override public int length() {
        return query.length;
      }

      @Override public int get(int i) {
        return query[i];
      }
    });
  }

  /**
   * Check if the two offsets belong to the same record.
   *
   * @param firstOffset  The first offset.
   * @param secondOffset The second offset.
   * @return True if the two offsets belong to the same record, false otherwise.
   */
  @Override public boolean sameRecord(long firstOffset, long secondOffset) {
    return offsetToRecordId((int) firstOffset) == offsetToRecordId((int) secondOffset);
  }

  /**
   * Search for all records that contain a particular regular expression.
   *
   * @param query The regular expression (UTF-8 encoded).
   * @return The records ids for records that contain the regular search expression.
   * @throws RegExParsingException
   */
  @Override public Integer[] recordSearchRegexIds(String query) throws RegExParsingException {
    Set<RegExMatch> regexOffsetResults = regexSearch(query);
    Set<Integer> recordIds = new HashSet<>();
    for (RegExMatch m : regexOffsetResults) {
      int recordId = offsetToRecordId((int) m.getOffset());
      if (!recordIds.contains(recordId)) {
        recordIds.add(recordId);
      }
    }
    return recordIds.toArray(new Integer[recordIds.size()]);
  }
}
