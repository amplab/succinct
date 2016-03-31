package edu.berkeley.cs.succinct.buffers.annot;

import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.util.SuccinctConstants;
import gnu.trove.list.array.TIntArrayList;

import java.io.DataInputStream;
import java.io.IOException;

public class AnnotatedSuccinctBuffer extends SuccinctFileBuffer {

  private static final char DELIM = '^';

  /**
   * Constructor to initialize from input byte array.
   *
   * @param input   The input byte array.
   */
  public AnnotatedSuccinctBuffer(byte[] input) {
    super(input);
  }

  /**
   * Constructor to load the data from a DataInputStream with specified file size.
   *
   * @param is Input stream to load the data from
   * @param fileSize Input stream to load the data from
   */
  public AnnotatedSuccinctBuffer(DataInputStream is, int fileSize) {
    try {
      readFromStream(is, fileSize);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private int readInteger(int offset) {
    byte[] bytes = extract(offset, SuccinctConstants.INT_SIZE_BYTES);
    return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
  }

  private int readInteger(int offset, int i) {
    int nBytes = SuccinctConstants.INT_SIZE_BYTES;
    byte[] bytes = extract(offset + i * nBytes, nBytes);
    return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
  }

  private short readShort(int offset) {
    byte[] bytes = extract(offset, SuccinctConstants.INT_SIZE_BYTES);
    return (short) (bytes[0] << 8 | (bytes[1] & 0xFF));
  }

  public AnnotationRecord findAnnotationRecord(String docId, String annotClass, String annotType) {
    // Find the record
    byte[] query = (DELIM + annotClass + DELIM + docId + DELIM + annotType + DELIM).getBytes();
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

    return new AnnotationRecord(offset, docId, annotClass, annotType, nEntries);
  }

  private int lowerBound(AnnotationRecord ar, int val) {
    int offset = ar.getOffset();
    int len = ar.getNumEntries();
    int lo = 0;
    int hi = len;

    while (lo != hi) {
      int mid = lo + (hi - lo) / 2;
      int arrVal = readInteger(offset, mid);
      if (arrVal <= val) {
        lo = mid + 1;
      }
      else {
        hi = mid;
      }
    }

    return lo - 1;
  }

  /*
  private int upperBound(AnnotationRecord ar, int val) {
    int offset = ar.getOffset();
    int len = ar.getNumEntries();
    int lo = 0;
    int hi = len - 1;
    int mid = lo + (hi - lo) / 2;
    while (true) {
      if (readInteger(offset, mid) > 0) {
        hi = mid - 1;
        if (hi < lo)
          return mid;
      } else {
        lo = mid + 1;
        if (hi < lo) {
          return mid < len - 1 ? mid + 1 : -1;
        }
      }
      mid = lo + (hi - lo) / 2;
    }
  }
  */

  public int[] findAnnotationsOver(AnnotationRecord ar, int begin, int end) {
    if (ar == null) {
      return new int[0];
    }

    int idx = lowerBound(ar, begin);
    if (idx < 0 || idx >= ar.getNumEntries()) {
      return new int[0];
    }

    TIntArrayList res = new TIntArrayList();
    while (idx < ar.getNumEntries()) {
      int rBegin = getRangeBegin(ar, idx);
      int rEnd = getRangeEnd(ar, idx);
      if (end < rBegin) break;
      if (begin >= rBegin && end <= rEnd) {
        res.add(idx);
      }
      idx++;
    }

    return res.toArray();
  }

  public int getRangeBegin(AnnotationRecord ar, int i) {
    if (ar == null) return -1;
    if (i < 0 || i >= ar.getNumEntries()) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + ar.getNumEntries() + " i = " + i);
    }
    int offset = ar.getOffset() + i * SuccinctConstants.INT_SIZE_BYTES;
    return readInteger(offset);
  }

  public int getRangeEnd(AnnotationRecord ar, int i) {
    if (ar == null) return -1;
    if (i < 0 || i >= ar.getNumEntries()) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + ar.getNumEntries() + " i = " + i);
    }
    int offset = ar.getOffset() + (ar.getNumEntries() + i) * SuccinctConstants.INT_SIZE_BYTES;
    return readInteger(offset);
  }

  public int getAnnotId(AnnotationRecord ar, int i) {
    if (ar == null) return -1;
    if (i < 0 || i >= ar.getNumEntries()) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + ar.getNumEntries() + " i = " + i);
    }
    int offset = ar.getOffset() + (2 * ar.getNumEntries() + i) * SuccinctConstants.INT_SIZE_BYTES;
    return readInteger(offset);
  }

  public String getMetadata(AnnotationRecord ar, int i) {
    if (ar == null) return null;
    if (i < 0 || i >= ar.getNumEntries()) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + ar.getNumEntries() + " i = " + i);
    }
    int curOffset = ar.getOffset() + (3 * ar.getNumEntries()) * SuccinctConstants.INT_SIZE_BYTES;
    while (true) {
      short length = readShort(curOffset);
      curOffset += SuccinctConstants.SHORT_SIZE_BYTES;
      if (i == 0) {
        return new String(extract(curOffset, length));
      }

      // Skip length bytes
      curOffset += length;
      i--;
    }
  }

  public Annotation getAnnotation(AnnotationRecord ar, int i) {
    if (ar == null) return null;
    if (i < 0 || i >= ar.getNumEntries()) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + ar.getNumEntries() + " i = " + i);
    }
    return new Annotation(ar.getDocId(), getAnnotId(ar, i), ar.getAnnotClass(), ar.getAnnotType(),
      getRangeBegin(ar, i), getRangeEnd(ar, i), getMetadata(ar, i));
  }
}
