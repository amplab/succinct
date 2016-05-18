package edu.berkeley.cs.succinct.buffers.annot;

import edu.berkeley.cs.succinct.util.SuccinctConstants;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class AnnotationIterator implements Iterator<Annotation> {

  private AnnotationRecord record;
  private int curIdx;
  private int end;

  public AnnotationIterator(AnnotationRecord record) {
    this.record = record;
    this.curIdx = 0;
    // Set end to the beginning of metadata
    this.end = record.getOffset() + 3 * SuccinctConstants.INT_SIZE_BYTES + record.getNumEntries();
  }

  public int nextRecordOffset() {
    if (hasNext()) {
      return -1;
    }
    return end;
  }

  @Override public boolean hasNext() {
    return curIdx < record.getNumEntries();
  }

  @Override public Annotation next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Annotation result = record.getAnnotation(curIdx++);
    end += result.getMetadata().length() + SuccinctConstants.SHORT_SIZE_BYTES;
    return result;
  }

  @Override public void remove() {
    throw new UnsupportedOperationException();
  }
}
