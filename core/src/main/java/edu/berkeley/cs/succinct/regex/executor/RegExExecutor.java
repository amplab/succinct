package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.regex.parser.RegEx;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * Boilerplate for the Regular Expression Executor.
 */
public abstract class RegExExecutor {

  protected SuccinctFile succinctFile;
  protected RegEx regEx;
  protected TreeSet<RegExMatch> finalResults;

  enum SortType {
    FRONT_SORTED,
    END_SORTED
  }

  /**
   * Constructor to initialize RegExExecutor.
   *
   * @param succinctFile The input SuccinctFile.
   * @param regEx The input regular expression.
   */
  RegExExecutor(SuccinctFile succinctFile, RegEx regEx) {
    this.succinctFile = succinctFile;
    this.regEx = regEx;
  }

  /**
   * Executes the regular expression.
   */
  public void execute() {
    finalResults = compute(regEx, SortType.FRONT_SORTED);
  }

  /**
   * Computes the regular expression subtree.
   *
   * @param r The regular expression.
   * @return The results for the subtree.
   */
  protected abstract TreeSet<RegExMatch> compute(RegEx r, SortType sortType);

  /**
   * Returns the set of final results.
   *
   * @return Final results for the regex query.
   */
  public TreeSet<RegExMatch> getFinalResults() {
    return finalResults;
  }

  /**
   * Allocates a TreeSet based on specified sorting scheme.
   *
   * @param sortType The sorting scheme.
   * @return An allocated TreeSet with specified sort scheme.
   */
  protected TreeSet<RegExMatch> allocateSet(SortType sortType) {
    switch (sortType) {
      case END_SORTED:
        return new TreeSet<>(RegExMatch.END_COMPARATOR);
      case FRONT_SORTED:
        return new TreeSet<>(RegExMatch.FRONT_COMPARATOR);
      default:
        return new TreeSet<>(RegExMatch.FRONT_COMPARATOR);
    }
  }

  /**
   * Computes the regular expression wildcard using the results from two regex sub-expressions.
   *
   * @param left     A set of (offset, length) pairs (END_SORTED).
   * @param right    A set of (offset, length) pairs (FRONT_SORTED).
   * @param sortType Sorting type for the returned set.
   * @return A set of (offset, length) pairs.
   */
  protected TreeSet<RegExMatch> regexWildcard(TreeSet<RegExMatch> left, TreeSet<RegExMatch> right,
    SortType sortType) {

    TreeSet<RegExMatch> wildcardRes = allocateSet(sortType);

    Iterator<RegExMatch> leftIterator = left.iterator();
    RegExMatch lowerBoundEntry = new RegExMatch(0, 0);
    while (leftIterator.hasNext()) {
      RegExMatch leftEntry = leftIterator.next();
      lowerBoundEntry.setOffset(leftEntry.end());
      RegExMatch rightEntry = right.ceiling(lowerBoundEntry);

      if (rightEntry == null)
        break;

      // Greedy match
      RegExMatch lastMatch = null;
      while (rightEntry != null && succinctFile.sameRecord(leftEntry.getOffset(), rightEntry.getOffset())) {
        lastMatch = rightEntry;
        rightEntry = right.higher(rightEntry);
      }

      if (lastMatch != null) {
        long distance = lastMatch.getOffset() - leftEntry.getOffset();
        wildcardRes
          .add(new RegExMatch(leftEntry.getOffset(), (int) distance + lastMatch.getLength()));
      }
    }

    return wildcardRes;
  }
}
