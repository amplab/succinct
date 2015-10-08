package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.RegexMatch;
import edu.berkeley.cs.succinct.regex.parser.*;

import java.util.Iterator;
import java.util.TreeSet;

public class RegExExecutor {

  private SuccinctFile succinctFile;
  private RegEx regEx;
  private TreeSet<RegexMatch> finalResults;


  enum SortType {
    FRONT_SORTED,
    END_SORTED
  }

  /**
   * Constructor to initialize Regex Executor with the Succinct Buffer and regex query.
   *
   * @param succinctFile The backing SuccinctBuffer.
   * @param regEx        The regular expression query.
   */
  public RegExExecutor(SuccinctFile succinctFile, RegEx regEx) {
    this.succinctFile = succinctFile;
    this.regEx = regEx;
  }

  /**
   * Executes the regular expression query using the backing SuccinctBuffer.
   */
  public void execute() {
    finalResults = compute(regEx, SortType.FRONT_SORTED);
  }

  /**
   * Returns the set of final results.
   *
   * @return Final results for the regex query.
   */
  public TreeSet<RegexMatch> getFinalResults() {
    return finalResults;
  }

  /**
   * Allocates a TreeSet based on specified sorting scheme.
   *
   * @param sortType The sorting scheme.
   * @return An allocated TreeSet with specified sort scheme.
   */
  private TreeSet<RegexMatch> allocateSet(SortType sortType) {
    switch (sortType) {
      case END_SORTED:
        return new TreeSet<RegexMatch>(RegexMatch.END_COMPARATOR);
      case FRONT_SORTED:
        return new TreeSet<RegexMatch>(RegexMatch.FRONT_COMPARATOR);
      default:
        return new TreeSet<RegexMatch>(RegexMatch.FRONT_COMPARATOR);
    }
  }

  /**
   * Computes the regular expression query by recursively running through the regex tree.
   *
   * @param r        The regular expression query.
   * @param sortType Sorting type for the returned set.
   * @return A set of (offset, length) pairs.
   */
  private TreeSet<RegexMatch> compute(RegEx r, SortType sortType) {
    switch (r.getRegExType()) {
      case Blank: {
        return allocateSet(sortType);
      }
      case Primitive: {
        return mgramSearch((RegExPrimitive) r, sortType);
      }
      case Union: {
        TreeSet<RegexMatch> firstRes = compute(((RegExUnion) r).getFirst(), sortType);
        TreeSet<RegexMatch> secondRes = compute(((RegExUnion) r).getSecond(), sortType);
        return regexUnion(firstRes, secondRes, sortType);
      }
      case Concat: {
        TreeSet<RegexMatch> leftRes = compute(((RegExConcat) r).getLeft(), SortType.END_SORTED);
        TreeSet<RegexMatch> rightRes = compute(((RegExConcat) r).getRight(), SortType.FRONT_SORTED);
        return regexConcat(leftRes, rightRes, sortType);
      }
      case Repeat: {
        TreeSet<RegexMatch> internalRes =
          compute(((RegExRepeat) r).getInternal(), SortType.END_SORTED);
        return regexRepeat(internalRes, ((RegExRepeat) r).getRegExRepeatType(), sortType);
      }
      case Wildcard: {
        TreeSet<RegexMatch> leftRes = compute(((RegExWildcard) r).getLeft(), SortType.END_SORTED);
        TreeSet<RegexMatch> rightRes =
          compute(((RegExWildcard) r).getRight(), SortType.FRONT_SORTED);
        return regexWildcard(leftRes, rightRes, sortType);
      }
      default:
        throw new UnsupportedOperationException("Unsupported operator");
    }
  }

  /**
   * Computes the regular expression search results for a primitive regex query.
   *
   * @param rp       Primitive regular expression.
   * @param sortType Sorting type for the returned set.
   * @return A set of (offset, length) pairs.
   */
  protected TreeSet<RegexMatch> mgramSearch(RegExPrimitive rp, SortType sortType) {
    TreeSet<RegexMatch> mgramRes = allocateSet(sortType);
    String mgram = rp.getMgram();
    Long[] searchRes = succinctFile.search(mgram.getBytes());
    for (int i = 0; i < searchRes.length; i++) {
      mgramRes.add(new RegexMatch(searchRes[i], mgram.length()));
    }
    return mgramRes;
  }

  /**
   * Computes the regular expression wildcard using the results from two regex sub-expressions.
   *
   * @param left     A set of (offset, length) pairs (END_SORTED).
   * @param right    A set of (offset, length) pairs (FRONT_SORTED).
   * @param sortType Sorting type for the returned set.
   * @return A set of (offset, length) pairs.
   */
  protected TreeSet<RegexMatch> regexWildcard(TreeSet<RegexMatch> left, TreeSet<RegexMatch> right,
    SortType sortType) {

    System.out.println("Executing wildcard...");
    TreeSet<RegexMatch> wildcardRes = allocateSet(sortType);

    Iterator<RegexMatch> leftIterator = left.iterator();
    RegexMatch lowerBoundEntry = new RegexMatch(0, 0);
    while (leftIterator.hasNext()) {
      RegexMatch leftEntry = leftIterator.next();
      lowerBoundEntry.setOffset(leftEntry.end());
      RegexMatch rightEntry = right.ceiling(lowerBoundEntry);
      if (rightEntry == null)
        continue;
      Iterator<RegexMatch> rightIterator = right.tailSet(rightEntry, true).iterator();
      while (rightIterator.hasNext()) {
        rightEntry = rightIterator.next();
        long distance = rightEntry.getOffset() - leftEntry.getOffset();
        wildcardRes
          .add(new RegexMatch(leftEntry.getOffset(), (int) distance + rightEntry.getLength()));
      }
    }

    return wildcardRes;
  }

  /**
   * Computes the regular expression union using the results from two regex sub-expressions.
   *
   * @param left     A set of (offset, length) pairs (FRONT_SORTED).
   * @param right    A set of (offset, length) pairs (FRONT_SORTED).
   * @param sortType Sorting type for the returned set.
   * @return A set of (offset, length) pairs.
   */
  protected TreeSet<RegexMatch> regexUnion(TreeSet<RegexMatch> left, TreeSet<RegexMatch> right,
    SortType sortType) {
    TreeSet<RegexMatch> unionRes = allocateSet(sortType);

    unionRes.addAll(left);
    unionRes.addAll(right);

    return unionRes;
  }

  /**
   * Computes the regex concatenation using the results from two regex sub-expressions.
   *
   * @param left     A set of (offset, length) pairs (END_SORTED).
   * @param right    A set of (offset, length) pairs (FRONT_SORTED).
   * @param sortType Sorting type for the returned set.
   * @return A set of (offset, length) pairs.
   */
  protected TreeSet<RegexMatch> regexConcat(TreeSet<RegexMatch> left, TreeSet<RegexMatch> right,
    SortType sortType) {
    TreeSet<RegexMatch> concatRes = allocateSet(sortType);

    if (left.isEmpty() || right.isEmpty())
      return concatRes;

    Iterator<RegexMatch> leftIterator = left.iterator();
    RegexMatch lowerBoundEntry = new RegexMatch(0, 0);
    while (leftIterator.hasNext()) {
      RegexMatch leftEntry = leftIterator.next();
      lowerBoundEntry.setOffset(leftEntry.end());
      RegexMatch rightEntry = right.ceiling(lowerBoundEntry);
      if (rightEntry == null || leftEntry.end() != rightEntry.begin())
        continue;
      do {
        concatRes.add(
          new RegexMatch(leftEntry.getOffset(), leftEntry.getLength() + rightEntry.getLength()));
        rightEntry = right.higher(rightEntry);
      } while (rightEntry != null && leftEntry.end() == rightEntry.begin());
    }

    return concatRes;
  }

  /**
   * Computes the regex repetition using the results from child regex sub-expression.
   *
   * @param child      A set of (offset, length) pairs (END_SORTED).
   * @param repeatType The type of repeat operation.
   * @param sortType   Sorting type for the returned set.
   * @return A set of (offset, length) pairs.
   */
  protected TreeSet<RegexMatch> regexRepeat(TreeSet<RegexMatch> child, RegExRepeatType repeatType,
    SortType sortType) {
    TreeSet<RegexMatch> repeatRes = allocateSet(sortType);
    TreeSet<RegexMatch> right = allocateSet(SortType.FRONT_SORTED);
    right.addAll(child);

    switch (repeatType) {
      case ZeroOrMore: {
        throw new UnsupportedOperationException("Zero or more unsupported.");
      }
      case OneOrMore: {
        TreeSet<RegexMatch> concatRes = child;
        repeatRes.addAll(child);
        do {
          concatRes = regexConcat(concatRes, right, sortType);
          repeatRes.addAll(concatRes);
        } while (concatRes.size() > 0);
        break;
      }
      case MinToMax: {
        throw new UnsupportedOperationException("Min to max unsupported.");
      }
    }
    return repeatRes;
  }
}
