package edu.berkeley.cs.succinct.regex.parser;

public class RegExConcat extends RegEx {

  RegEx first;
  RegEx second;

  /**
   * Constructor to initialize a RegExConcat from two input regular expressions.
   *
   * @param first  The first regular expression.
   * @param second The second regular expression.
   */
  public RegExConcat(RegEx first, RegEx second) {
    super(RegExType.Concat);
    this.first = first;
    this.second = second;
  }

  /**
   * Get the first regular expression.
   *
   * @return The first regular expression.
   */
  public RegEx getFirst() {
    return first;
  }

  /**
   * Get the second regular expression.
   *
   * @return The second regular expression.
   */
  public RegEx getSecond() {
    return second;
  }

}
