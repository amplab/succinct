package edu.berkeley.cs.succinct.buffers.annot;

import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.util.SuccinctConstants;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;

public class AnnotatedSuccinctBuffer extends SuccinctFileBuffer {

  private static final char DELIM = '^';

  /**
   * Constructor to initialize from input byte array.
   *
   * @param input The input byte array.
   */
  public AnnotatedSuccinctBuffer(byte[] input) {
    super(input);
  }

  /**
   * Constructor to load the data from a DataInputStream with specified file size.
   *
   * @param is       Input stream to load the data from
   * @param fileSize Input stream to load the data from
   */
  public AnnotatedSuccinctBuffer(DataInputStream is, int fileSize) {
    try {
      readFromStream(is, fileSize);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public int readInteger(int offset) {
    byte[] bytes = extractBytes(offset, SuccinctConstants.INT_SIZE_BYTES);
    return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
  }

  public int readInteger(int offset, int i) {
    int nBytes = SuccinctConstants.INT_SIZE_BYTES;
    byte[] bytes = extractBytes(offset + i * nBytes, nBytes);
    return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
  }

  public short readShort(int offset) {
    byte[] bytes = extractBytes(offset, SuccinctConstants.INT_SIZE_BYTES);
    return (short) (bytes[0] << 8 | (bytes[1] & 0xFF));
  }

  public AnnotationRecord getAnnotationRecord(String docId, String annotClass, String annotType) {
    // Find the record
    byte[] query = (DELIM + annotClass + DELIM + annotType + DELIM + docId + DELIM).getBytes();
    Long[] queryRes = search(query);
    assert queryRes.length == 1 || queryRes.length == 0;

    if (queryRes.length == 0) {
      return null;
    }

    // Extract num entries
    int nEntriesOffset = queryRes[0].intValue() + query.length;
    int nEntries = readInteger(nEntriesOffset);

    // Get offset to data
    int offset = nEntriesOffset + SuccinctConstants.INT_SIZE_BYTES;

    return new AnnotationRecord(offset, docId, annotClass, annotType, nEntries, this);
  }

  public Iterator<AnnotationRecord> getAnnotationRecords(final String annotClass,
    final String annotType) {
    // Find the record
    final byte[] query = (DELIM + annotClass + DELIM + annotType + DELIM).getBytes();
    final Long[] queryRes = search(query);

    final AnnotationRecord[] records = new AnnotationRecord[queryRes.length];

    return new Iterator<AnnotationRecord>() {
      int i = 0;

      @Override public boolean hasNext() {
        return i < queryRes.length;
      }

      @Override public AnnotationRecord next() {
        // Extract num entries
        int docIdOffset = queryRes[i].intValue() + query.length;
        String docId = extractUntil(docIdOffset, DELIM);
        int nEntriesOffset = docIdOffset + docId.length() + 1;
        int nEntries = readInteger(nEntriesOffset);

        // Get offset to data
        int offset = nEntriesOffset + SuccinctConstants.INT_SIZE_BYTES;

        return new AnnotationRecord(offset, docId, annotClass, annotType, nEntries,
          AnnotatedSuccinctBuffer.this);
      }

      @Override public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
