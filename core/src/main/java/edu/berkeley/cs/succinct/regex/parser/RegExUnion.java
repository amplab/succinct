package edu.berkeley.cs.succinct.regex.parser;

public class RegExUnion extends RegEx {
  private RegEx first;
  private RegEx second;

  /**
   * Constructor to initialize RegExUnion from two regular expressions.
   *
   * @param first  The left regular expression.
   * @param second The right regular expression.
   */
  public RegExUnion(RegEx first, RegEx second) {
    super(RegExType.Union);
    this.first = first;
    this.second = second;
  }

  /**
   * Get the left regular expression.
   *
   * @return The left regular expression.
   */
  public RegEx getFirst() {
    return first;
  }

  /**
   * Get the right regular expression.
   *
   * @return The right regular expression.
   */
  public RegEx getSecond() {
    return second;
  }
}
