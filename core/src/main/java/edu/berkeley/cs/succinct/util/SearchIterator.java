package edu.berkeley.cs.succinct.util;

import edu.berkeley.cs.succinct.SuccinctCore;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class SearchIterator implements Iterator<Long> {
  private Range searchRange;
  private SuccinctCore succinctCore;

  public SearchIterator(SuccinctCore succinctCore, Range searchRange) {
    this.searchRange = searchRange;
    this.succinctCore = succinctCore;
  }

  @Override public boolean hasNext() {
    return !searchRange.empty();
  }

  @Override public Long next() {
    if (searchRange.empty()) {
      throw new NoSuchElementException("No more results in search iterator.");
    }

    long offset = succinctCore.lookupSA(searchRange.begin());
    searchRange.advanceBeginning();
    return offset;
  }

  @Override public void remove() {
    throw new UnsupportedOperationException("Remove is not supported on the search iterator");
  }
}
