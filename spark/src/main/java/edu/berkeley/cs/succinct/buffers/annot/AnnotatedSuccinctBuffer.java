package edu.berkeley.cs.succinct.buffers.annot;

import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.util.SuccinctConstants;

import java.io.DataInputStream;
import java.io.IOException;

public class AnnotatedSuccinctBuffer extends SuccinctFileBuffer {

  public static final char DELIM = '^';

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
   * @param fileSize Size of the file.
   */
  public AnnotatedSuccinctBuffer(DataInputStream is, int fileSize) {
    try {
      readFromStream(is, fileSize);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public AnnotationRecord getAnnotationRecord(String docId) {
    // Find the record
    byte[] query = (DELIM + docId + DELIM).getBytes();
    Long[] queryRes = search(query);
    assert queryRes.length == 1 || queryRes.length == 0;

    if (queryRes.length == 0) {
      return null;
    }

    // Extract num entries
    int nEntriesOffset = queryRes[0].intValue() + query.length;
    int nEntries = extractInt(nEntriesOffset);

    // Get offset to data
    int offset = nEntriesOffset + SuccinctConstants.INT_SIZE_BYTES;

    return new AnnotationRecord(offset, docId, nEntries, this);
  }
}
