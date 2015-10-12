package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.regex.parser.*;

import java.util.Iterator;
import java.util.TreeSet;

public class BlackBoxRegExExecutor extends RegExExecutor {

  /**
   * Constructor to initialize Regex Executor with the Succinct Buffer and regex query.
   *
   * @param succinctFile The backing SuccinctBuffer.
   * @param regEx        The regular expression query.
   */
  public BlackBoxRegExExecutor(SuccinctFile succinctFile, RegEx regEx) {
    super(succinctFile, regEx);

  }

  /**
   * Computes the regular expression query by recursively running through the regex tree.
   *
   * @param r        The regular expression query.
   * @param sortType Sorting type for the returned set.
   * @return A set of (offset, length) pairs.
   */
  @Override protected TreeSet<RegExMatch> compute(RegEx r, SortType sortType) {
    switch (r.getRegExType()) {
      case Blank: {
        return null;
      }
      case Primitive: {
        return mgramSearch((RegExPrimitive) r, sortType);
      }
      case Union: {
        TreeSet<RegExMatch> firstRes = compute(((RegExUnion) r).getFirst(), sortType);
        TreeSet<RegExMatch> secondRes = compute(((RegExUnion) r).getSecond(), sortType);
        return regexUnion(firstRes, secondRes, sortType);
      }
      case Concat: {
        TreeSet<RegExMatch> leftRes = compute(((RegExConcat) r).getLeft(), SortType.END_SORTED);
        TreeSet<RegExMatch> rightRes = compute(((RegExConcat) r).getRight(), SortType.FRONT_SORTED);
        return regexConcat(leftRes, rightRes, sortType);
      }
      case Repeat: {
        TreeSet<RegExMatch> internalRes =
          compute(((RegExRepeat) r).getInternal(), SortType.END_SORTED);
        return regexRepeat(internalRes, ((RegExRepeat) r).getRegExRepeatType(), sortType);
      }
      case Wildcard: {
        TreeSet<RegExMatch> leftRes = compute(((RegExWildcard) r).getLeft(), SortType.END_SORTED);
        TreeSet<RegExMatch> rightRes =
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
  protected TreeSet<RegExMatch> mgramSearch(RegExPrimitive rp, SortType sortType) {
    TreeSet<RegExMatch> mgramRes = allocateSet(sortType);
    String mgram = rp.getPrimitiveStr();
    Long[] searchRes = succinctFile.search(mgram.getBytes());
    for (int i = 0; i < searchRes.length; i++) {
      mgramRes.add(new RegExMatch(searchRes[i], mgram.length()));
    }
    return mgramRes;
  }

  /**
   * Computes the regular expression char range using the results from two regex sub-expressions.
   *
   * @param left     A set of (offset, length) pairs (END_SORTED).
   * @param right    A set of (offset, length) pairs (FRONT_SORTED).
   * @param sortType Sorting type for the returned set.
   * @return A set of (offset, length) pairs.
   */
  protected TreeSet<RegExMatch> regexCharRange(TreeSet<RegExMatch> left, TreeSet<RegExMatch> right,
    String charRange, boolean repeat, SortType sortType) {

    TreeSet<RegExMatch> charRangeRes = allocateSet(sortType);

    if (left == null && right == null) {
      for (char c: charRange.toCharArray()) {
        Long[] searchRes = succinctFile.search(String.valueOf(c).getBytes());
        for (Long offset : searchRes) {
          charRangeRes.add(new RegExMatch(offset, 1));
        }
      }
      if (repeat) {
        charRangeRes = regexRepeat(charRangeRes, RegExRepeatType.OneOrMore, sortType);
      }
    } else if (left == null) {
      if (right.isEmpty()) {
        return charRangeRes;
      }

      if (repeat) {
        Iterator<RegExMatch> rightIterator = right.iterator();
        while (rightIterator.hasNext()) {
          RegExMatch rightEntry = rightIterator.next();
          int i = 1;
          while (true) {
            char c = succinctFile.charAt(rightEntry.begin() - i);
            if (charRange.indexOf(c) == -1) {
              break;
            }
            charRangeRes
              .add(new RegExMatch(rightEntry.getOffset() - i, rightEntry.getLength() + i));
            i++;
          }
        }
      } else {
        Iterator<RegExMatch> rightIterator = right.iterator();
        while (rightIterator.hasNext()) {
          RegExMatch rightEntry = rightIterator.next();
          char c = succinctFile.charAt(rightEntry.begin() - 1);
          if (charRange.indexOf(c) != -1) {
            charRangeRes
              .add(new RegExMatch(rightEntry.getOffset() - 1, rightEntry.getLength() + 1));
          }
        }
      }
    } else if (right == null) {
      if (left.isEmpty()) {
        return charRangeRes;
      }

      if (repeat) {
        Iterator<RegExMatch> leftIterator = left.iterator();
        while (leftIterator.hasNext()) {
          RegExMatch leftEntry = leftIterator.next();
          int i = 1;
          while (true) {
            char c = succinctFile.charAt(leftEntry.end() + i - 1);
            if (charRange.indexOf(c) == -1) {
              break;
            }
            charRangeRes.add(new RegExMatch(leftEntry.getOffset(), leftEntry.getLength() + i));
            i++;
          }
        }
      } else {
        Iterator<RegExMatch> leftIterator = left.iterator();
        while (leftIterator.hasNext()) {
          RegExMatch leftEntry = leftIterator.next();
          char c = succinctFile.charAt(leftEntry.end());
          if (charRange.indexOf(c) != -1) {
            charRangeRes.add(new RegExMatch(leftEntry.getOffset(), leftEntry.getLength() + 1));
          }
        }
      }
    } else {
      if (right.isEmpty() || right.isEmpty()) {
        return charRangeRes;
      }

      TreeSet<RegExMatch> leftAug = allocateSet(SortType.END_SORTED);
      if (repeat) {
        Iterator<RegExMatch> leftIterator = left.iterator();
        while (leftIterator.hasNext()) {
          RegExMatch leftEntry = leftIterator.next();
          int i = 1;
          while (true) {
            char c = succinctFile.charAt(leftEntry.end() + i - 1);
            if (charRange.indexOf(c) == -1) {
              break;
            }
            leftAug.add(new RegExMatch(leftEntry.getOffset(), leftEntry.getLength() + i));
            i++;
          }
        }
      } else {
        Iterator<RegExMatch> leftIterator = left.iterator();
        while (leftIterator.hasNext()) {
          RegExMatch leftEntry = leftIterator.next();
          char c = succinctFile.charAt(leftEntry.end());
          if (charRange.indexOf(c) != -1) {
            leftAug.add(new RegExMatch(leftEntry.getOffset(), leftEntry.getLength() + 1));
          }
        }
      }

      charRangeRes = regexConcat(leftAug, right, sortType);
    }

    return charRangeRes;
  }

  /**
   * Computes the regular expression union using the results from two regex sub-expressions.
   *
   * @param left     A set of (offset, length) pairs (FRONT_SORTED).
   * @param right    A set of (offset, length) pairs (FRONT_SORTED).
   * @param sortType Sorting type for the returned set.
   * @return A set of (offset, length) pairs.
   */
  protected TreeSet<RegExMatch> regexUnion(TreeSet<RegExMatch> left, TreeSet<RegExMatch> right,
    SortType sortType) {
    TreeSet<RegExMatch> unionRes = allocateSet(sortType);

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
  protected TreeSet<RegExMatch> regexConcat(TreeSet<RegExMatch> left, TreeSet<RegExMatch> right,
    SortType sortType) {
    TreeSet<RegExMatch> concatRes = allocateSet(sortType);

    if (left.isEmpty() || right.isEmpty())
      return concatRes;

    Iterator<RegExMatch> leftIterator = left.iterator();
    RegExMatch lowerBoundEntry = new RegExMatch(0, 0);
    while (leftIterator.hasNext()) {
      RegExMatch leftEntry = leftIterator.next();
      lowerBoundEntry.setOffset(leftEntry.end());
      RegExMatch rightEntry = right.ceiling(lowerBoundEntry);
      if (rightEntry == null || leftEntry.end() != rightEntry.begin())
        continue;
      do {
        concatRes.add(
          new RegExMatch(leftEntry.getOffset(), leftEntry.getLength() + rightEntry.getLength()));
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
  protected TreeSet<RegExMatch> regexRepeat(TreeSet<RegExMatch> child, RegExRepeatType repeatType,
    SortType sortType) {
    TreeSet<RegExMatch> repeatRes = allocateSet(sortType);
    TreeSet<RegExMatch> right = allocateSet(SortType.FRONT_SORTED);
    right.addAll(child);

    switch (repeatType) {
      case ZeroOrMore: {
        throw new UnsupportedOperationException("Zero or more unsupported.");
      }
      case OneOrMore: {
        TreeSet<RegExMatch> concatRes = child;
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
