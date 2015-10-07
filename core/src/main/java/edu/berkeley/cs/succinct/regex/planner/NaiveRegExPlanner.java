package edu.berkeley.cs.succinct.regex.planner;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.parser.RegEx;

public class NaiveRegExPlanner extends RegExPlanner {

  /**
   * Constructor to initialize NaiveRegExPlanner with the backing Succinct Buffer and the regex query.
   *
   * @param succinctFile The backing Succinct Buffer.
   * @param regEx        The regex query.
   */
  public NaiveRegExPlanner(SuccinctFile succinctFile, RegEx regEx) {
    super(succinctFile, regEx);
  }

  /**
   * Generates a plan for regular expression execution.
   * The naive planner simply returns the original regular expression.
   *
   * @return The regex execution plan.
   */
  @Override public RegEx plan() {
    return regEx;
  }
}
