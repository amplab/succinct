package edu.berkeley.cs.succinct.regex.parser;

public class RegExCharRange extends RegEx {
  private RegEx left;
  private RegEx right;
  private String charRange;
  private boolean repeat;

  /**
   * Constructor to initialize a RegExConcat from two input regular expressions.
   *
   * @param left  The left regular expression.
   * @param right The right regular expression.
   */
  public RegExCharRange(RegEx left, RegEx right, String charRange, boolean repeat) {
    super(RegExType.CharRange);
    this.left = left;
    this.right = right;
    this.charRange = charRange;
    this.repeat = repeat;
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

  /**
   * Get the character range.
   *
   * @return The character range.
   */
  public String getCharRange() {
    return charRange;
  }

  /**
   * Get whether the character range is repeated.
   *
   * @return True if the character range is repeated, false otherwise.
   */
  public boolean isRepeat() {
    return repeat;
  }
}
