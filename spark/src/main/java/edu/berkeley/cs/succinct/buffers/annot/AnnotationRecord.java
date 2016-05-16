package edu.berkeley.cs.succinct.buffers.annot;

import edu.berkeley.cs.succinct.util.SuccinctConstants;
import gnu.trove.list.array.TIntArrayList;

public class AnnotationRecord {
  private int offset;
  private String annotClass;
  private String annotType;
  private String docId;
  private int numEntries;
  private AnnotatedSuccinctBuffer buffer;

  public AnnotationRecord(int offset, String docId, String annotClass, String annotType,
    int numEntries, AnnotatedSuccinctBuffer buffer) {
    this.offset = offset;
    this.docId = docId;
    this.annotClass = annotClass;
    this.annotType = annotType;
    this.numEntries = numEntries;
    this.buffer = buffer;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public String getDocId() {
    return docId;
  }

  public void setDocId(String docId) {
    this.docId = docId;
  }

  public String getAnnotClass() {
    return annotClass;
  }

  public void setAnnotClass(String annotClass) {
    this.annotClass = annotClass;
  }

  public String getAnnotType() {
    return annotType;
  }

  public void setAnnotType(String annotType) {
    this.annotType = annotType;
  }

  public int getNumEntries() {
    return numEntries;
  }

  public void setNumEntries(int numEntries) {
    this.numEntries = numEntries;
  }

  public int lowerBound(int startOffset) {
    int lo = 0;
    int hi = numEntries;

    while (lo != hi) {
      int mid = lo + (hi - lo) / 2;
      int arrVal = buffer.readInteger(offset, mid);
      if (arrVal <= startOffset) {
        lo = mid + 1;
      } else {
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

  public int[] findAnnotationsOver(int begin, int end) {
    int idx = lowerBound(begin);
    if (idx < 0 || idx >= numEntries) {
      return new int[0];
    }

    TIntArrayList res = new TIntArrayList();
    while (idx < numEntries) {
      int rBegin = getRangeBegin(idx);
      int rEnd = getRangeEnd(idx);
      if (end < rBegin)
        break;
      if (begin >= rBegin && end <= rEnd) {
        res.add(idx);
      }
      idx++;
    }

    return res.toArray();
  }

  public int getRangeBegin(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }
    int rbOffset = offset + i * SuccinctConstants.INT_SIZE_BYTES;
    return buffer.readInteger(rbOffset);
  }

  public int getRangeEnd(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }
    int reOffset = offset + (numEntries + i) * SuccinctConstants.INT_SIZE_BYTES;
    return buffer.readInteger(reOffset);
  }

  public int getAnnotId(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }
    int aiOffset = offset + (2 * numEntries + i) * SuccinctConstants.INT_SIZE_BYTES;
    return buffer.readInteger(aiOffset);
  }

  public String getMetadata(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }
    int curOffset = offset + (3 * numEntries) * SuccinctConstants.INT_SIZE_BYTES;
    while (true) {
      short length = buffer.readShort(curOffset);
      curOffset += SuccinctConstants.SHORT_SIZE_BYTES;
      if (i == 0) {
        return buffer.extract(curOffset, length);
      }

      // Skip length bytes
      curOffset += length;
      i--;
    }
  }

  public Annotation getAnnotation(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }
    return new Annotation(docId, getAnnotId(i), annotClass, annotType, getRangeBegin(i),
      getRangeEnd(i), getMetadata(i));
  }
}
