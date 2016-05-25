package edu.berkeley.cs.succinct.regex;

import edu.berkeley.cs.succinct.util.container.Range;

public class SuccinctRegExMatch extends Range {

  private int length;

  /**
   * Constructor to initialize Regex Match.
   *
   * @param first  First element.
   * @param second Second element.
   * @param length The length of the match.
   */
  public SuccinctRegExMatch(long first, long second, int length) {
    super(first, second);
    this.length = length;
  }

  /**
   * Constructor to initialize Regex Match.
   *
   * @param range Input range.
   * @param length Length of the match.
   */
  public SuccinctRegExMatch(Range range, int length) {
    this(range.begin(), range.end(), length);
  }

  /**
   * Get the length for the match.
   *
   * @return The length for the match.
   */
  public int getLength() {
    return length;
  }

  @Override public int hashCode() {
    int hash = 23;
    hash = (int) (hash * 31 + first);
    hash = (int) (hash * 31 + second);
    hash = hash * 31 + length;
    return hash;
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof SuccinctRegExMatch))
      return false;
    if (obj == this)
      return true;

    SuccinctRegExMatch rhs = (SuccinctRegExMatch) obj;
    return begin() == rhs.begin() && end() == rhs.end() && getLength() == rhs.getLength();
  }
}
