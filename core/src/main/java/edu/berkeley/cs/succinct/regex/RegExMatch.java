package edu.berkeley.cs.succinct.regex;

import java.util.Comparator;

public class RegExMatch implements Comparable<RegExMatch> {

  public static final Comparator<RegExMatch> END_COMPARATOR = new Comparator<RegExMatch>() {
    @Override public int compare(RegExMatch o1, RegExMatch o2) {
      long o1End = (o1.getOffset() + o1.getLength());
      long o2End = (o2.getOffset() + o2.getLength());
      if (o1End == o2End) {
        if (o1.getLength() == o2.getLength()) {
          return 0;
        } else if (o1.getLength() < o2.getLength()) {
          return -1;
        } else {
          return 1;
        }
      }
      long offDiff = o1End - o2End;
      return (offDiff < 0L) ? -1 : 1;
    }
  };

  public static final Comparator<RegExMatch> FRONT_COMPARATOR = new Comparator<RegExMatch>() {
    @Override public int compare(RegExMatch o1, RegExMatch o2) {
      if (o1.getOffset() == o2.getOffset()) {
        if (o1.getLength() == o2.getLength()) {
          return 0;
        } else if (o1.getLength() < o2.getLength()) {
          return -1;
        } else {
          return 1;
        }
      }
      long offDiff = o1.getOffset() - o2.getOffset();
      return (offDiff < 0L) ? -1 : 1;
    }
  };

  private long offset;
  private int length;

  public RegExMatch(long offset, int length) {
    this.offset = offset;
    this.length = length;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public long begin() {
    return offset;
  }

  public long end() {
    return offset + length;
  }

  public boolean before(RegExMatch r) {
    return end() <= r.getOffset();
  }

  public boolean contains(RegExMatch r) {
    return r.begin() >= begin() && r.end() <= end();
  }

  @Override public String toString() {
    return "(" + offset + ", " + length + ")";
  }

  @Override public int compareTo(RegExMatch o) {
    if (offset == o.getOffset()) {
      return length - o.getLength();
    }
    long offDiff = (offset - o.getOffset());
    return (offDiff < 0L) ? -1 : 1;
  }
}
