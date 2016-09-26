package edu.berkeley.cs.succinct.util.container;

import java.io.Serializable;

/**
 * Represents a numeric range, [first, second] (inclusive). It is an
 * invalid/empty range iff second < first.
 */
public class Range implements Comparable<Range>, Serializable {
  public long first, second;

  /**
   * Constructor to initialize pair
   *
   * @param first  First element.
   * @param second Second element.
   */
  public Range(long first, long second) {
    this.first = first;
    this.second = second;
  }

  public boolean contains(long value) {
    return value >= first && value <= second;
  }

  public boolean contains(long first, long second) {
    return first >= this.first && second <= this.second;
  }

  public long begin() {
    return first;
  }

  public long end() {
    return second;
  }

  public void advanceBeginning() {
    if (!empty()) {
      first++;
    }
  }

  public long size() {
    return second - first + 1;
  }

  public boolean empty() {
    return first > second;
  }

  @Override public int compareTo(Range that) {
    long diff1 = this.first - that.first;
    long diff2 = this.second - that.second;
    if (diff1 == 0) {
      return diff2 < 0 ? -1 : (diff2 == 0 ? 0 : 1);
    } else {
      return diff1 < 0 ? -1 : 1;
    }
  }

  @Override public int hashCode() {
    int hash = 23;
    hash = (int) (hash * 31 + first);
    hash = (int) (hash * 31 + second);
    return hash;
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof Range))
      return false;
    if (obj == this)
      return true;

    Range rhs = (Range) obj;
    return begin() == rhs.begin() && end() == rhs.end();
  }

  @Override public String toString() {
    return String.format("[%d, %d]", first, second);
  }
}
