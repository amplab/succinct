package edu.berkeley.cs.succinct.buffers.annot;

import edu.berkeley.cs.succinct.util.SuccinctConstants;
import gnu.trove.list.array.TIntArrayList;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class AnnotationRecord {
  private int offset;
  private String docId;
  private int numEntries;
  private SuccinctAnnotationBuffer buf;

  public AnnotationRecord(int offset, String docId, int numEntries, SuccinctAnnotationBuffer buf) {
    this.offset = offset;
    this.docId = docId;
    this.numEntries = numEntries;
    this.buf = buf;
  }

  /**
   * Get the underlying SuccinctAnnotationBuffer.
   *
   * @return The underlying SuccinctAnnotationBuffer.
   */
  public SuccinctAnnotationBuffer getBuf() {
    return buf;
  }

  /**
   * Get the offset to the beginning of the AnnotationRecord in SuccinctAnnotationBuffer.
   *
   * @return The offset to the beginning of the AnnotationRecord.
   */
  public int getOffset() {
    return offset;
  }

  /**
   * Get the Annotation Class.
   *
   * @return The Annotation Class.
   */
  public String getAnnotClass() {
    return buf.getAnnotClass();
  }

  /**
   * Get the Annotation Type.
   *
   * @return The Annotation Type.
   */
  public String getAnnotType() {
    return buf.getAnnotType();
  }

  /**
   * Get the document ID for the AnnotationRecord.
   *
   * @return The documentID for the AnnotationRecord.
   */
  public String getDocId() {
    return docId;
  }

  /**
   * Get the number of Annotations encoded in the AnnotationRecord.
   *
   * @return The number of Annotations encoded in the AnnotationRecord.
   */
  public int getNumEntries() {
    return numEntries;
  }

  /**
   * Get the start offset for the ith annotation.
   *
   * @param i The index for the annotation.
   * @return The start offset.
   */
  public int getStartOffset(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }
    int rbOffset = offset + i * SuccinctConstants.INT_SIZE_BYTES;
    return buf.extractInt(rbOffset);
  }

  /**
   * Get the end offset for the ith annotation.
   *
   * @param i The index for the annotation.
   * @return The end offset.
   */
  public int getEndOffset(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }
    int reOffset = offset + (numEntries + i) * SuccinctConstants.INT_SIZE_BYTES;
    return buf.extractInt(reOffset);
  }

  /**
   * Get the annotation ID for the ith annotation.
   *
   * @param i The index for the annotation.
   * @return The annotation ID.
   */
  public int getAnnotId(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }
    int aiOffset = offset + (2 * numEntries + i) * SuccinctConstants.INT_SIZE_BYTES;
    return buf.extractInt(aiOffset);
  }

  /**
   * Get the metadata for the ith annotation.
   *
   * @param i The index for the annotation.
   * @return The annotation metadata.
   */
  public String getMetadata(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }
    int curOffset = offset + (3 * numEntries) * SuccinctConstants.INT_SIZE_BYTES;
    while (true) {
      short length = buf.extractShort(curOffset);
      curOffset += SuccinctConstants.SHORT_SIZE_BYTES;
      if (i == 0) {
        return buf.extract(curOffset, length);
      }

      // Skip length bytes
      curOffset += length;
      i--;
    }
  }

  /**
   * Get the ith annotation.
   *
   * @param i The index for the annotation.
   * @return The annotation.
   */
  public Annotation getAnnotation(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }
    return new Annotation(getAnnotClass(), getAnnotType(), docId, getAnnotId(i), getStartOffset(i),
      getEndOffset(i), getMetadata(i));
  }

  /**
   * Get an iterator over all annotations in the record.
   *
   * @return Iterator over all annotations in the record.
   */
  public AnnotationIterator getAnnotationIterator() {
    return new AnnotationIterator(this, false);
  }

  /**
   * Find the first start offset <= the given offset.
   *
   * @param offset The offset to search.
   * @return The location of the first start offset <= offset.
   */
  public int firstLEQ(int offset) {
    int lo = 0, hi = numEntries, arrVal;

    while (lo != hi) {
      int mid = lo + (hi - lo) / 2;
      arrVal = buf.extractInt(this.offset + mid * SuccinctConstants.INT_SIZE_BYTES);
      if (arrVal <= offset)
        lo = mid + 1;
      else
        hi = mid;
    }

    return lo - 1;
  }

  /**
   * Find the first start offset >= the given offset.
   *
   * @param offset The offset to search.
   * @return The location of the first start offset >= offset.
   */
  public int firstGEQ(int offset) {
    int lo = 0, hi = numEntries, arrVal = 0;

    while (lo != hi) {
      int mid = lo + (hi - lo) / 2;
      arrVal = buf.extractInt(this.offset + mid * SuccinctConstants.INT_SIZE_BYTES);
      if (arrVal < offset)
        lo = mid + 1;
      else
        hi = mid;
    }

    return lo;
  }

  /**
   * Find annotations containing the range (begin, end).
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @return The matching annotations.
   */
  public Iterator<Annotation> annotationsContaining(final int begin, final int end) {
    return new Iterator<Annotation>() {
      int idx = -1;
      int startOffset;
      int endOffset;

      private void advanceToNextValid() {
        boolean valid;
        do {
          if (++idx == numEntries)
            break;
          startOffset = getStartOffset(idx);
          if (startOffset > begin)
            break;
          endOffset = getEndOffset(idx);
          valid = end <= endOffset;
        } while (!valid);
      }

      {
        advanceToNextValid();
      }

      private Annotation getCurrentAnnotation() {
        return new Annotation(buf.getAnnotClass(), buf.getAnnotType(), docId, getAnnotId(idx),
          startOffset, endOffset, getMetadata(idx));
      }

      @Override public boolean hasNext() {
        return idx < numEntries && startOffset <= begin;
      }

      @Override public Annotation next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        Annotation toReturn = getCurrentAnnotation();
        advanceToNextValid();
        return toReturn;
      }

      @Override public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Find annotations contained in the range (begin, end).
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @return The matching annotations.
   */
  public int[] annotationsContainedIn(int begin, int end) {
    int idx = firstGEQ(begin);
    if (idx < 0 || idx >= numEntries) {
      return new int[0];
    }

    TIntArrayList res = new TIntArrayList();
    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (startOffset > end)
        break;
      if (startOffset >= begin && endOffset <= end) {
        res.add(idx);
      }
      idx++;
    }

    return res.toArray();
  }

}
