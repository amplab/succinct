package edu.berkeley.cs.succinct.buffers.annot;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class AnnotationIterator implements Iterator<Annotation> {

  private SuccinctAnnotationBuffer buf;
  private AnnotationRecord record;
  private boolean multiRecord;
  private int curRecIdx;
  private int curAnnotIdx;

  public AnnotationIterator(SuccinctAnnotationBuffer buf) {
    this.buf = buf;
    this.curRecIdx = 0;
    this.record = buf.getAnnotationRecord(curRecIdx);
    this.curAnnotIdx = 0;
    this.multiRecord = true;
  }

  public AnnotationIterator(AnnotationRecord record) {
    this.buf = null;
    this.curRecIdx = Integer.MIN_VALUE;
    this.record = record;
    this.curAnnotIdx = 0;
    this.multiRecord = false;
  }

  @Override public boolean hasNext() {
    return record != null;
  }

  @Override public Annotation next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Annotation result = record.getAnnotation(curAnnotIdx);
    curAnnotIdx++;
    if (curAnnotIdx == record.getNumEntries()) {
      curAnnotIdx = 0;
      record = multiRecord ? buf.getAnnotationRecord(++curRecIdx) : null;
    }
    return result;
  }

  @Override public void remove() {
    throw new UnsupportedOperationException();
  }
}
