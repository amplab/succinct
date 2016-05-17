package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctIndexedFile;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.util.Source;
import edu.berkeley.cs.succinct.util.SuccinctConstants;
import edu.berkeley.cs.succinct.util.container.Range;
import edu.berkeley.cs.succinct.util.iterator.SearchIterator;
import edu.berkeley.cs.succinct.util.iterator.SearchRecordIterator;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class SuccinctIndexedFileBuffer extends SuccinctFileBuffer implements SuccinctIndexedFile {

  private static final long serialVersionUID = -8357331195541317163L;
  protected transient int[] offsets;

  private transient ByteBuffer readBufferIndexed;

  /**
   * Constructor to initialize SuccinctIndexedBuffer from input byte array and offsets corresponding to records.
   *
   * @param input   The input byte array.
   * @param offsets Offsets corresponding to records.
   */
  public SuccinctIndexedFileBuffer(byte[] input, int[] offsets) {
    super(input);
    this.offsets = offsets;
  }

  /**
   * Constructor to initialize SuccinctIndexedBuffer from input byte array and offsets corresponding to records.
   *
   * @param input   The input byte array.
   * @param offsets Offsets corresponding to records.
   */
  public SuccinctIndexedFileBuffer(char[] input, int[] offsets) {
    super(input);
    this.offsets = offsets;
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
      readFromStream(is);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Constructor to load the data from a DataInputStream with specified file size.
   *
   * @param is Input stream to load the data from
   * @param fileSize Input stream to load the data from
   */
  public SuccinctIndexedFileBuffer(DataInputStream is, int fileSize) {
    try {
      readFromStream(is, fileSize);
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
    mapFromBuffer(buf);
  }

  @Override public int getCompressedSize() {
    if (readBufferIndexed != null) {
      return readBufferIndexed.capacity();
    }
    return super.getCompressedSize()
      + (12 + offsets.length * SuccinctConstants.INT_SIZE_BYTES);
  }

  /**
   * Get the number of records.
   *
   * @return The number of records.
   */
  @Override public int getNumRecords() {
    return offsets.length;
  }

  /**
   * Get the offset for a given recordId
   *
   * @param recordId The record id.
   * @return The corresponding offset.
   */
  @Override public int getRecordOffset(int recordId) {
    if (recordId >= offsets.length || recordId < 0) {
      throw new ArrayIndexOutOfBoundsException("Record does not exist: recordId = " + recordId);
    }

    return offsets[recordId];
  }

  /**
   * Get the ith record.
   *
   * @param recordId The record index.
   * @return The corresponding record.
   */
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

  /**
   * Write Succinct data structures to a DataOutputStream.
   *
   * @param os Output stream to write data to.
   * @throws IOException
   */
  @Override public void writeToStream(DataOutputStream os) throws IOException {
    super.writeToStream(os);
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
    int len = is.readInt();
    offsets = new int[len];
    for (int i = 0; i < len; i++) {
      offsets[i] = is.readInt();
    }
  }

  @Override public void readFromStream(DataInputStream is, int fileSize) throws IOException {
    byte[] data = new byte[fileSize];
    is.readFully(data);
    readBufferIndexed = ByteBuffer.wrap(data);
    mapFromBuffer(readBufferIndexed);
  }

  /**
   * Reads Succinct data structures from a ByteBuffer.
   *
   * @param buf ByteBuffer to read Succinct data structures from.
   */
  @Override public void mapFromBuffer(ByteBuffer buf) {
    super.mapFromBuffer(buf);
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
    oos.writeObject(offsets);
  }

  /**
   * Deserialize SuccinctIndexedBuffer from InputStream.
   *
   * @param ois ObjectInputStream to read from.
   * @throws IOException
   */
  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    offsets = (int[]) ois.readObject();
  }
}
