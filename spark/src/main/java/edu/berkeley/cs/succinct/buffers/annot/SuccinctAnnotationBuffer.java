package edu.berkeley.cs.succinct.buffers.annot;

import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.util.SuccinctConstants;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;

public class SuccinctAnnotationBuffer extends SuccinctFileBuffer {

  public static final char DELIM = '^';

  /**
   * Constructor to initialize from input byte array.
   *
   * @param input The input byte array.
   */
  public SuccinctAnnotationBuffer(byte[] input) {
    super(input);
  }

  /**
   * Constructor to load the data from a DataInputStream with specified file size.
   *
   * @param is       Input stream to load the data from
   * @param fileSize Size of the file.
   */
  public SuccinctAnnotationBuffer(DataInputStream is, int fileSize) {
    try {
      readFromStream(is, fileSize);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public Iterator<Annotation> iterator() {
    return new Iterator<Annotation>() {

      @Override public boolean hasNext() {
        return false;
      }

      @Override public Annotation next() {
        return null;
      }

      @Override public void remove() {

      }
    };
  }

  public AnnotationRecord getAnnotationRecord(int recordOffset) {
    assert charAt(recordOffset) == DELIM;
    String docId = extractUntil(recordOffset + 1, DELIM);

    // Extract num entries
    int nEntriesOffset = recordOffset + docId.length() + 2;
    int nEntries = extractInt(nEntriesOffset);

    // Get offset to data
    int dataOffset = nEntriesOffset + SuccinctConstants.INT_SIZE_BYTES;

    return new AnnotationRecord(dataOffset, docId, nEntries, this);
  }

  /**
   * Get the annotation record for a given document ID.
   *
   * @param docId The document ID.
   * @return The annotation record corresponding to the document ID.
   */
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
    int dataOffset = nEntriesOffset + SuccinctConstants.INT_SIZE_BYTES;

    return new AnnotationRecord(dataOffset, docId, nEntries, this);
  }
}
