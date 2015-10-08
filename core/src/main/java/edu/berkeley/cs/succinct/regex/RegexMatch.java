package edu.berkeley.cs.succinct.regex;

import java.util.Comparator;

public class RegexMatch implements Comparable<RegexMatch> {

  public static final Comparator<RegexMatch> END_COMPARATOR = new Comparator<RegexMatch>() {
    @Override public int compare(RegexMatch o1, RegexMatch o2) {
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

  public static final Comparator<RegexMatch> FRONT_COMPARATOR = new Comparator<RegexMatch>() {
    @Override public int compare(RegexMatch o1, RegexMatch o2) {
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

  public RegexMatch(long offset, int length) {
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

  public boolean adjacentAfter(RegexMatch r) {
    return offset == r.end();
  }

  public boolean adjacentBefore(RegexMatch r) {
    return r.getOffset() == end();
  }

  public boolean after(RegexMatch r) {
    return offset >= r.end();
  }

  public boolean before(RegexMatch r) {
    return end() <= r.getOffset();
  }

  @Override
  public String toString() {
    return "(" + offset + ", " + length + ")";
  }

  @Override
  public int compareTo(RegexMatch o) {
    if (offset == o.getOffset()) {
      return length - o.getLength();
    }
    long offDiff = (offset - o.getOffset());
    return (offDiff < 0L) ? -1 : 1;
  }
}
