package edu.berkeley.cs.succinct.regex.parser;

public class RegExWildcard extends RegEx {

  RegEx left;
  RegEx right;

  /**
   * Constructor to initialize a RegExWildcard from two input regular expressions.
   *
   * @param left  The left regular expression.
   * @param right The right regular expression.
   */
  public RegExWildcard(RegEx left, RegEx right) {
    super(RegExType.Wildcard);
    this.left = left;
    this.right = right;
  }

  /**
   * Get the left regular expression.
   *
   * @return The left regular expression.
   */
  public RegEx getLeft() {
    return left;
  }

  /**
   * Get the right regular expression.
   *
   * @return The right regular expression.
   */
  public RegEx getRight() {
    return right;
  }

}
