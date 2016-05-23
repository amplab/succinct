package edu.berkeley.cs.succinct.buffers.annot;

import edu.berkeley.cs.succinct.util.SuccinctConstants;

import java.util.ArrayList;

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
  public Annotation getAnnotation(final int i) {
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
    return new AnnotationIterator(this);
  }

  /**
   * Find the first start offset <= the given offset.
   *
   * @param offset The offset to search.
   * @return The location of the first start offset <= offset.
   */
  public int firstLEQ(final int offset) {
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
  public int firstGEQ(final int offset) {
    int lo = 0, hi = numEntries, arrVal;

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
  public Annotation[] annotationsContaining(final int begin, final int end) {
    int idx = 0;
    ArrayList<Annotation> res = new ArrayList<>();
    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (startOffset > begin)
        break;
      if (begin >= startOffset && end <= endOffset) {
        res.add(new Annotation(getAnnotClass(), getAnnotType(), docId, getAnnotId(idx), startOffset,
          endOffset, getMetadata(idx)));
      }
      idx++;
    }
    return res.toArray(new Annotation[res.size()]);
  }

  /**
   * Checks if any annotation in the record contains the input range.
   *
   * @param begin  Beginning of the input range.
   * @param end    End of the input range.
   * @param filter Filter on annotation metadata.
   * @return True if the record has any annotation containing the range; false otherwise.
   */
  public boolean contains(final int begin, final int end, final MetadataFilter filter) {
    int idx = 0;
    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (startOffset > begin)
        break;
      if (begin >= startOffset && end <= endOffset && filter.filter(getMetadata(idx))) {
        return true;
      }
      idx++;
    }
    return false;
  }

  /**
   * Find annotations contained in the range (begin, end).
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @return The matching annotations.
   */
  public Annotation[] annotationsContainedIn(final int begin, final int end) {
    int idx = firstGEQ(begin);
    if (idx < 0 || idx >= numEntries) {
      return new Annotation[0];
    }

    ArrayList<Annotation> res = new ArrayList<>();
    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (startOffset > end)
        break;
      if (startOffset >= begin && endOffset <= end) {
        res.add(new Annotation(getAnnotClass(), getAnnotType(), docId, getAnnotId(idx), startOffset,
          endOffset, getMetadata(idx)));
      }
      idx++;
    }

    return res.toArray(new Annotation[res.size()]);
  }

  /**
   * Checks if any annotation in the record is contained in the input range.
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @return True if the record has any annotation containing the range; false otherwise.
   */
  public boolean containedIn(final int begin, final int end, final MetadataFilter filter) {
    int idx = firstGEQ(begin);
    if (idx < 0 || idx >= numEntries) {
      return false;
    }

    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (startOffset > end)
        break;
      if (startOffset >= begin && endOffset <= end && filter.filter(getMetadata(idx))) {
        return true;
      }
      idx++;
    }

    return false;
  }

  /**
   * Find annotations before the range (begin, end), within `range` chars of begin.
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @param range Max number of chars the annotation can be away from begin; -1 sets the limit to
   *              infinity, i.e., all annotations before.
   * @return The matching annotations.
   */
  public Annotation[] annotationsBefore(final int begin, final int end, final int range) {
    int idx = firstLEQ(begin);
    if (idx < 0 || idx >= numEntries) {
      return new Annotation[0];
    }

    ArrayList<Annotation> res = new ArrayList<>();
    while (idx >= 0) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (endOffset <= begin && !(range != -1 && begin - endOffset > range)) {
        res.add(new Annotation(getAnnotClass(), getAnnotType(), docId, getAnnotId(idx), startOffset,
          endOffset, getMetadata(idx)));
      }
      idx--;
    }

    return res.toArray(new Annotation[res.size()]);
  }

  /**
   * Checks if any annotation in the record is before the input range, but within `range` characters
   * of the start.
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @param range Max number of chars the annotation can be away from end; -1 sets the limit to
   *              infinity, i.e., all annotations after.
   * @return True if the record has any annotation before the range; false otherwise.
   */
  public boolean before(final int begin, final int end, final int range,
    final MetadataFilter filter) {
    int idx = firstLEQ(begin);
    if (idx < 0 || idx >= numEntries) {
      return false;
    }

    while (idx >= 0) {
      int endOffset = getEndOffset(idx);
      if (endOffset <= begin && !(range != -1 && begin - endOffset > range) &&
        filter.filter(getMetadata(idx))) {
        return true;
      }
      idx--;
    }

    return false;
  }

  /**
   * Find annotations after the range (begin, end), within `range` chars of end.
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @param range Max number of chars the annotation can be away from end; -1 sets the limit to
   *              infinity, i.e., all annotations after.
   * @return The matching annotations.
   */
  public Annotation[] annotationsAfter(final int begin, final int end, final int range) {
    int idx = firstGEQ(end);
    if (idx >= numEntries) {
      return new Annotation[0];
    }

    ArrayList<Annotation> res = new ArrayList<>();
    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (range != -1 && startOffset - end > range)
        break;
      res.add(new Annotation(getAnnotClass(), getAnnotType(), docId, getAnnotId(idx), startOffset,
        endOffset, getMetadata(idx)));
      idx++;
    }

    return res.toArray(new Annotation[res.size()]);
  }

  /**
   * Checks if any annotation in the record is after the input range, but within `range` characters
   * of the end.
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @param range Max number of chars the annotation can be away from end; -1 sets the limit to
   *              infinity, i.e., all annotations after.
   * @return True if the record has any annotation after the range; false otherwise.
   */
  public boolean after(final int begin, final int end, final int range,
    final MetadataFilter filter) {
    int idx = firstGEQ(end);
    if (idx >= numEntries)
      return false;

    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      if (range != -1 && startOffset - end > range)
        break;
      if (filter.filter(getMetadata(idx)))
        return true;
      idx++;
    }
    return false;
  }

}
