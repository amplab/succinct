package edu.berkeley.cs.succinct.buffers.annot;

import edu.berkeley.cs.succinct.util.SuccinctConstants;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class AnnotationIterator implements Iterator<Annotation> {

  private AnnotationRecord record;
  private boolean multiRecord;
  private int curIdx;
  private int curEnd;

  public AnnotationIterator(AnnotationRecord rec, boolean multiRecord) {
    this.record = rec;
    this.curIdx = 0;
    // Set end to the beginning of metadata
    this.curEnd = rec.getOffset() + 3 * SuccinctConstants.INT_SIZE_BYTES * rec.getNumEntries();
    this.multiRecord = multiRecord;
  }

  @Override public boolean hasNext() {
    return record != null;
  }

  @Override public Annotation next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Annotation result = record.getAnnotation(curIdx);
    curEnd += result.getMetadata().length() + SuccinctConstants.SHORT_SIZE_BYTES;
    curIdx++;
    if (curIdx == record.getNumEntries()) {
      curIdx = 0;
      record = multiRecord ? record.getBuf().getAnnotationRecord(curEnd) : null;
      if (record != null)
        curEnd = record.getOffset() + 3 * SuccinctConstants.INT_SIZE_BYTES * record.getNumEntries();
    }
    return result;
  }

  @Override public void remove() {
    throw new UnsupportedOperationException();
  }
}
