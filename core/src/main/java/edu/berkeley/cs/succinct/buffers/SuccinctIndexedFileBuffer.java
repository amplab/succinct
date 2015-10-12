package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctIndexedFile;
import edu.berkeley.cs.succinct.dictionary.Tables;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.util.Range;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class SuccinctIndexedFileBuffer extends SuccinctFileBuffer implements SuccinctIndexedFile {

  private static final long serialVersionUID = -8357331195541317163L;
  protected transient int[] offsets;
  protected transient long firstRecordId;

  /**
   * Constructor to initialize SuccinctIndexedBuffer from input byte array, offsets corresponding to records,
   * context length and file offset.
   *
   * @param input         The input byte array.
   * @param offsets       Offsets corresponding to records.
   * @param contextLen    Context Length.
   * @param fileOffset    Beginning offset for this file chunk (if file is partitioned).
   * @param firstRecordId First record id for this partition (if file is partitioned).
   */
  public SuccinctIndexedFileBuffer(byte[] input, int[] offsets, int contextLen, long fileOffset,
    long firstRecordId) {
    super(input, contextLen, fileOffset);
    this.offsets = offsets;
    this.firstRecordId = firstRecordId;
  }

  /**
   * Constructor to initialize SuccinctIndexedBuffer from input byte array, offsets corresponding to records,
   * context length and file offset.
   *
   * @param input      The input byte array.
   * @param offsets    Offsets corresponding to records.
   * @param fileOffset Beginning offset for this file chunk (if file is partitioned).
   */
  public SuccinctIndexedFileBuffer(byte[] input, int[] offsets, long fileOffset,
    long firstRecordId) {
    this(input, offsets, 3, fileOffset, firstRecordId);
    this.offsets = offsets;
    this.firstRecordId = firstRecordId;
  }

  /**
   * Constructor to initialize SuccinctIndexedBuffer from input byte array, offsets corresponding to records and file
   * offset.
   *
   * @param input      The input byte array.
   * @param offsets    Offsets corresponding to records.
   * @param fileOffset Beginning offset for this file chunk (if file is partitioned).
   */
  public SuccinctIndexedFileBuffer(byte[] input, int[] offsets, long fileOffset) {
    this(input, offsets, fileOffset, 0);
  }

  /**
   * Constructor to initialize SuccinctIndexedBuffer from input byte array and offsets corresponding to records.
   *
   * @param input   The input byte array.
   * @param offsets Offsets corresponding to records.
   */
  public SuccinctIndexedFileBuffer(byte[] input, int[] offsets) {
    this(input, offsets, 0);
  }

  /**
   * Constructor to load the data from persisted Succinct data-structures.
   *
   * @param path        Path to load data from.
   * @param storageMode Mode in which data is stored (In-memory or Memory-mapped)
   */
  public SuccinctIndexedFileBuffer(String path, StorageMode storageMode) {
    super(path, storageMode);
  }

  /**
   * Constructor to load the data from a DataInputStream.
   *
   * @param is Input stream to load the data from
   */
  public SuccinctIndexedFileBuffer(DataInputStream is) {
    try {
      Tables.init();
      readFromStream(is);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Constructor to load the data from a ByteBuffer.
   *
   * @param buf Input buffer to load the data from
   */
  public SuccinctIndexedFileBuffer(ByteBuffer buf) {
    super(buf);
  }

  /**
   * Get the number of records.
   *
   * @return The number of records.
   */
  public int getNumRecords() {
    return offsets.length;
  }

  /**
   * Get the first record id in this partition.
   *
   * @return The first record id in this partition.
   */
  public long getFirstRecordId() {
    return firstRecordId;
  }

  /**
   * Get the range of record ids in this partition.
   *
   * @return The range of record ids in this partition.
   */
  public Range getRecordIdRange() {
    return new Range(firstRecordId, firstRecordId + getNumRecords() - 1);
  }

  /**
   * Get the ith record.
   *
   * @param partitionRecordId The record index.
   * @return The corresponding record.
   */
  @Override public byte[] getPartitionRecord(int partitionRecordId) {
    if (partitionRecordId >= offsets.length || partitionRecordId < 0) {
      throw new ArrayIndexOutOfBoundsException(
        "Record does not exist: partitionRecordId = " + partitionRecordId);
    }
    int begOffset = offsets[partitionRecordId];
    int endOffset = (partitionRecordId == offsets.length - 1) ?
      getOriginalSize() - 1 :
      offsets[partitionRecordId + 1];
    int len = (endOffset - begOffset - 1);
    return extract(fileOffset + begOffset, len);
  }

  /**
   * Get the ith record.
   *
   * @param recordId The record index.
   * @return The corresponding record.
   */
  @Override public byte[] getRecord(long recordId) {
    int partitionRecordId = (int) (recordId - firstRecordId);
    return getPartitionRecord(partitionRecordId);
  }

  /**
   * Search for offset corresponding to a position in the input.
   *
   * @param pos Position in the input
   * @return Offset corresponding to the position.
   */
  @Override public int offsetToRecordId(int pos) {
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

  /**
   * Search for an input query and return offsets of all matching records.
   *
   * @param query Input query.
   * @return Offsets of all matching records.
   */
  @Override public Long[] recordSearchOffsets(byte[] query) {
    Set<Long> results = new HashSet<Long>();
    Range range = bwdSearch(query);

    long sp = range.first, ep = range.second;
    if (ep - sp + 1 <= 0) {
      return new Long[0];
    }

    for (long i = 0; i < ep - sp + 1; i++) {
      results.add(fileOffset + offsets[offsetToRecordId((int) lookupSA(sp + i))]);
    }

    return results.toArray(new Long[results.size()]);
  }

  /**
   * Count of all records containing a particular query.
   *
   * @param query Input query.
   * @return Count of all records containing input query.
   */
  @Override public long recordCount(byte[] query) {
    return recordSearchOffsets(query).length;
  }

  /**
   * Search for all records that contains the query.
   *
   * @param query Input query.
   * @return All records containing input query.
   */
  @Override public byte[][] recordSearch(byte[] query) {
    Set<Integer> recordIds = new HashSet<Integer>();
    ArrayList<byte[]> results = new ArrayList<byte[]>();
    Range range = bwdSearch(query);

    long sp = range.first, ep = range.second;
    if (ep - sp + 1 <= 0) {
      return new byte[0][0];
    }

    for (long i = 0; i < ep - sp + 1; i++) {
      long saVal = lookupSA(sp + i);
      int recordId = offsetToRecordId((int) saVal);
      if (!recordIds.contains(recordId)) {
        results.add(getPartitionRecord(recordId));
        recordIds.add(recordId);
      }
    }

    return results.toArray(new byte[results.size()][]);
  }

  /**
   * Performs a range search for all records that contains a substring between queryBegin and queryEnd.
   *
   * @param queryBegin The beginning of query range.
   * @param queryEnd   The end of query range.
   * @return All records matching the query range.
   */
  @Override public byte[][] recordRangeSearch(byte[] queryBegin, byte[] queryEnd) {
    Set<Integer> recordIds = new HashSet<Integer>();
    ArrayList<byte[]> results = new ArrayList<byte[]>();
    Range rangeBegin = bwdSearch(queryBegin);
    Range rangeEnd = bwdSearch(queryEnd);

    long sp = rangeBegin.first, ep = rangeEnd.second;
    if (ep - sp + 1 <= 0) {
      return new byte[0][0];
    }

    for (long i = 0; i < ep - sp + 1; i++) {
      long saVal = lookupSA(sp + i);
      int recordId = offsetToRecordId((int) saVal);
      if (!recordIds.contains(recordId)) {
        results.add(getPartitionRecord(recordId));
        recordIds.add(recordId);
      }
    }

    return results.toArray(new byte[results.size()][]);
  }

  /**
   * Perform multiple searches with different query types and return the intersection of the results.
   *
   * @param queryTypes The QueryType corresponding to each query
   * @param queries    The actual query parameters associated with each query
   * @return The records matching the multi-search queries.
   */
  @Override public byte[][] multiSearch(QueryType[] queryTypes, byte[][][] queries) {
    assert (queryTypes.length == queries.length);
    Set<Integer> recordIds = new HashSet<Integer>();
    ArrayList<byte[]> results = new ArrayList<byte[]>();

    if (queries.length == 0) {
      throw new IllegalArgumentException("multiSearch called with empty queries");
    }

    // Get all ranges
    ArrayList<Range> ranges = new ArrayList<Range>();
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
          Range rangeBegin, rangeEnd;
          rangeBegin = bwdSearch(queryBegin);
          rangeEnd = bwdSearch(queryEnd);
          range = new Range(rangeBegin.first, rangeEnd.second);
          break;
        }
        default: {
          throw new UnsupportedOperationException("Unsupported QueryType");
        }
      }

      if (range.second - range.first + 1 > 0) {
        ranges.add(range);
      } else {
        return new byte[0][0];
      }
    }
    int numRanges = ranges.size();

    Collections.sort(ranges, new RangeSizeComparator());

    // Populate the set of offsets corresponding to the first range
    Range firstRange = ranges.get(0);
    Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
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

    for (int recordId : recordIds) {
      if (counts.get(recordId) == numRanges) {
        results.add(getPartitionRecord(recordId));
      }
    }

    return results.toArray(new byte[results.size()][]);
  }

  /**
   * Check if the two offsets belong to the same record.
   *
   * @param firstOffset The first offset.
   * @param secondOffset The second offset.
   * @return True if the two offsets belong to the same record, false otherwise.
   */
  @Override public boolean sameRecord(long firstOffset, long secondOffset) {
    int firstChunkOffset = (int) (firstOffset - fileOffset);
    int secondChunkOffset = (int) (secondOffset - fileOffset);
    return offsetToRecordId(firstChunkOffset) == offsetToRecordId(secondChunkOffset);
  }

  /**
   * Search for all records that contain a particular regular expression.
   *
   * @param query The regular expression (UTF-8 encoded).
   * @return The records that contain the regular search expression.
   * @throws RegExParsingException
   */
  @Override public byte[][] recordSearchRegex(String query) throws RegExParsingException {
    Map<Long, Integer> regexOffsetResults = regexSearch(query);
    Set<Integer> recordIds = new HashSet<Integer>();
    ArrayList<byte[]> results = new ArrayList<byte[]>();
    for (Long offset : regexOffsetResults.keySet()) {
      int recordId = offsetToRecordId((int) (offset - fileOffset));
      if (!recordIds.contains(recordId)) {
        results.add(getPartitionRecord(recordId));
        recordIds.add(recordId);
      }
    }
    return results.toArray(new byte[results.size()][]);
  }

  /**
   * Extract a part of all records.
   *
   * @param offset Offset into record.
   * @param length Length of part to be extracted.
   * @return Extracted data.
   */
  @Override public byte[][] extractRecords(int offset, int length) {
    byte[][] records = new byte[offsets.length][];
    for (int i = 0; i < records.length; i++) {
      int curOffset = offsets[i] + offset;
      int nextOffset = (i == records.length - 1) ? getOriginalSize() : offsets[i + 1];
      length = (length < nextOffset - curOffset - 1) ? length : nextOffset - curOffset - 1;
      records[i] = extract(curOffset, length);
    }
    return records;
  }

  /**
   * Write Succinct data structures to a DataOutputStream.
   *
   * @param os Output stream to write data to.
   * @throws IOException
   */
  @Override public void writeToStream(DataOutputStream os) throws IOException {
    super.writeToStream(os);
    os.writeLong(firstRecordId);
    os.writeInt(offsets.length);
    for (int i = 0; i < offsets.length; i++) {
      os.writeInt(offsets[i]);
    }
  }

  /**
   * Reads Succinct data structures from a DataInputStream.
   *
   * @param is Stream to read data structures from.
   * @throws IOException
   */
  @Override public void readFromStream(DataInputStream is) throws IOException {
    super.readFromStream(is);
    firstRecordId = is.readLong();
    int len = is.readInt();
    offsets = new int[len];
    for (int i = 0; i < len; i++) {
      offsets[i] = is.readInt();
    }
  }

  /**
   * Reads Succinct data structures from a ByteBuffer.
   *
   * @param buf ByteBuffer to read Succinct data structures from.
   */
  @Override public void mapFromBuffer(ByteBuffer buf) {
    super.mapFromBuffer(buf);
    firstRecordId = buf.getLong();
    int len = buf.getInt();
    offsets = new int[len];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = buf.getInt();
    }
  }

  /**
   * Serialize SuccinctIndexedBuffer to OutputStream.
   *
   * @param oos ObjectOutputStream to write to.
   * @throws IOException
   */
  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.writeLong(firstRecordId);
    oos.writeObject(offsets);
  }

  /**
   * Deserialize SuccinctIndexedBuffer from InputStream.
   *
   * @param ois ObjectInputStream to read from.
   * @throws IOException
   */
  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    firstRecordId = ois.readLong();
    offsets = (int[]) ois.readObject();
  }
}
