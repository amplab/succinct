package edu.berkeley.cs.succinct.regex;

import edu.berkeley.cs.succinct.util.Range;

public class SuccinctRegExMatch extends Range {

  private int length;

  /**
   * Constructor to initialize Regex Match
   *
   * @param first  First element.
   * @param second Second element.
   * @param length The length of the match.
   */
  public SuccinctRegExMatch(long first, long second, int length) {
    super(first, second);
    this.length = length;
  }

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
}
