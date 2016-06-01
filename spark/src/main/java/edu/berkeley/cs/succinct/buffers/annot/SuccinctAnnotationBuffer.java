package edu.berkeley.cs.succinct.buffers.annot;

import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
import edu.berkeley.cs.succinct.util.SuccinctConstants;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class SuccinctAnnotationBuffer extends SuccinctIndexedFileBuffer {

  public static final char DELIM = '^';
  private transient String annotClass;
  private transient String annotType;
  private transient int[] docIdIndexes;
  private transient ByteBuffer readAnnotBuffer;

  /**
   * Constructor to initialize from input byte array.
   *
   * @param input The input byte array.
   */
  public SuccinctAnnotationBuffer(String annotClass, String annotType, int[] docIdIndexes, int[] annotationOffsets, byte[] input) {
    super(input, annotationOffsets);
    this.annotClass = annotClass;
    this.annotType = annotType;
    this.docIdIndexes = docIdIndexes;
  }

  /**
   * Constructor to load the data from a DataInputStream with specified file size.
   *
   * @param is       Input stream to load the data from
   * @param fileSize Size of the file.
   */
  public SuccinctAnnotationBuffer(String annotClass, String annotType, DataInputStream is, int fileSize) {
    try {
      readFromStream(is, fileSize);
    } catch (IOException e) {
      e.printStackTrace();
    }
    this.annotClass = annotClass;
    this.annotType = annotType;
  }

  /**
   * Get the size of the Succinct compressed file.
   *
   * @return The size of the Succinct compressed file.
   */
  @Override public int getCompressedSize() {
    if (readAnnotBuffer != null) {
      return readAnnotBuffer.capacity();
    }
    return super.getCompressedSize() + (12
      + docIdIndexes.length * SuccinctConstants.INT_SIZE_BYTES);
  }

  /**
   * Get the Annotation Class.
   *
   * @return The Annotation Class.
   */
  public String getAnnotClass() {
    return annotClass;
  }

  /**
   * Get the Annotation Type.
   *
   * @return The Annotation Type
   */
  public String getAnnotType() {
    return annotType;
  }

  /**
   * Get the docId index for a given annotation record index.
   *
   * @param recordIdx Annotation record index.
   * @return The document ID index.
   */
  public int getDocIdIndex(int recordIdx) {
    return docIdIndexes[recordIdx];
  }

  /**
   * Get the annotation record offset given the document ID index.
   *
   * @param docIdIdx The document ID index.
   * @return The corresponding annotation record offset.
   */
  public int getAnnotationRecordOffset(int docIdIdx) {
    if (docIdIdx < 0)
      return -1;
    int offsetIdx = Arrays.binarySearch(docIdIndexes, 0, docIdIndexes.length, docIdIdx);
    if (offsetIdx < 0)
      return -1;
    return getRecordOffset(offsetIdx);
  }

  /**
   * Get the annotation record for a given annotation record index.
   *
   * @param recordIdx The annotation record index.
   * @return The annotation record corresponding to the record index.
   */
  public AnnotationRecord getAnnotationRecord(int recordIdx, String docId) {
    if (recordIdx < 0 || recordIdx >= getNumRecords()) {
      return null;
    }

    // Extract num entries
    int nEntriesOffset = getRecordOffset(recordIdx);
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
  public AnnotationRecord getAnnotationRecord(String docId, int docIdIndex) {
    int recordOffset = getAnnotationRecordOffset(docIdIndex);
    if (recordOffset < 0) {
      return null;
    }

    // Extract num entries
    int nEntries = extractInt(recordOffset);

    // Get offset to data
    int dataOffset = recordOffset + SuccinctConstants.INT_SIZE_BYTES;

    return new AnnotationRecord(dataOffset, docId, nEntries, this);
  }

  /**
   * Write Succinct data structures to a DataOutputStream.
   *
   * @param os Output stream to write data to.
   * @throws IOException
   */
  @Override public void writeToStream(DataOutputStream os) throws IOException {
    super.writeToStream(os);
    os.writeInt(docIdIndexes.length);
    for (int i = 0; i < docIdIndexes.length; i++) {
      os.writeInt(docIdIndexes[i]);
    }
  }

  /**
   * Read data from stream.
   *
   * @param is       Input stream to read from.
   * @param fileSize The size of the file.
   * @throws IOException
   */
  @Override public void readFromStream(DataInputStream is, int fileSize) throws IOException {
    byte[] data = new byte[fileSize];
    is.readFully(data);
    readAnnotBuffer = ByteBuffer.wrap(data);
    mapFromBuffer(readAnnotBuffer);
  }

  /**
   * Reads Succinct data structures from a ByteBuffer.
   *
   * @param buf ByteBuffer to read Succinct data structures from.
   */
  @Override public void mapFromBuffer(ByteBuffer buf) {
    super.mapFromBuffer(buf);
    int len = buf.getInt();
    docIdIndexes = new int[len];
    for (int i = 0; i < docIdIndexes.length; i++) {
      docIdIndexes[i] = buf.getInt();
    }
  }

  /**
   * Serialize SuccinctIndexedBuffer to OutputStream.
   *
   * @param oos ObjectOutputStream to write to.
   * @throws IOException
   */
  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.writeObject(annotClass);
    oos.writeObject(annotType);
    oos.writeObject(docIdIndexes);
  }

  /**
   * Deserialize SuccinctIndexedBuffer from InputStream.
   *
   * @param ois ObjectInputStream to read from.
   * @throws IOException
   */
  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    annotClass = (String) ois.readObject();
    annotType = (String) ois.readObject();
    docIdIndexes = (int[]) ois.readObject();
  }
}
