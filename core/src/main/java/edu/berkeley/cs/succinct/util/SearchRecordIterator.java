package edu.berkeley.cs.succinct.util;

import edu.berkeley.cs.succinct.SuccinctIndexedFile;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class SearchRecordIterator implements Iterator<Integer> {

  private SearchIterator it;
  private SuccinctIndexedFile succinctFile;
  private Set<Integer> seenSoFar;
  private int lookaheadRecordId;

  public SearchRecordIterator(SearchIterator it, SuccinctIndexedFile succinctFile) {
    this.it = it;
    this.succinctFile = succinctFile;
    this.seenSoFar = new HashSet<>();
    this.lookaheadRecordId = -1;
  }

  private int nextRecordId() {
    return succinctFile.offsetToRecordId(it.next().intValue());
  }

  private boolean lookAhead() {
    // If lookahead record id isn't invalid, return true.
    // This is because we must have already looked ahead previously and not
    // used the result.
    if (lookaheadRecordId != -1) {
      return true;
    }

    // If the lookahead record id is invalid, then lookAhead to find the next
    // unique recordId. If there are none, return false.
    while (it.hasNext()) {
      int recordId = nextRecordId();
      if (!seenSoFar.contains(recordId)) {
        lookaheadRecordId = recordId;
        seenSoFar.add(recordId);
        return true;
      }
    }
    return false;
  }

  @Override public boolean hasNext() {
    return lookAhead();
  }

  @Override public Integer next() {
    if (lookAhead()) {
      int recordId = lookaheadRecordId;
      lookaheadRecordId = -1;
      return recordId;
    } else {
      throw new NoSuchElementException("No more records.");
    }
  }

  @Override public void remove() {
    throw new UnsupportedOperationException("Remove not supported on search record iterator.");
  }
}
