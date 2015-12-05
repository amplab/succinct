package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.SuccinctCore;
import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.SuccinctRegExMatch;
import edu.berkeley.cs.succinct.regex.parser.*;
import edu.berkeley.cs.succinct.util.container.Range;

import java.util.TreeSet;

public class SuccinctBwdRegExExecutor extends SuccinctRegExExecutor {

  private byte[] alphabet;

  /**
   * Constructor to initialize SuccinctFwdRegExExecutor.
   *
   * @param succinctFile The input SuccinctCore.
   * @param regEx        The input regular expression.
   */
  public SuccinctBwdRegExExecutor(SuccinctFile succinctFile, RegEx regEx) {
    super(succinctFile, regEx);
    this.alphabet = succinctFile.getAlphabet();
  }

  /**
   * Uses Succinct data representation (backward search) to compute regular expression.
   *
   * @param r Regular expression to compute.
   * @return The results of the regular expression.
   */
  @Override protected TreeSet<SuccinctRegExMatch> computeSuccinctly(RegEx r) {
    TreeSet<SuccinctRegExMatch> results = new TreeSet<>();
    switch (r.getRegExType()) {
      case Blank: {
        break;
      }
      case Primitive: {
        RegExPrimitive p = (RegExPrimitive) r;
        switch (p.getPrimitiveType()) {
          case MGRAM: {
            String mgram = p.getPrimitiveStr();
            Range range = succinctFile.bwdSearch(mgram.getBytes());
            if (!range.empty()) {
              results.add(new SuccinctRegExMatch(range, mgram.length()));
            }
            break;
          }
          case DOT: {
            for (byte b : alphabet) {
              char c = (char) b;
              if ((b == SuccinctCore.EOL) || (b == SuccinctCore.EOF || (b == SuccinctCore.EOA))) {
                continue;
              }
              Range range = succinctFile.bwdSearch(String.valueOf(c).getBytes());
              if (!range.empty()) {
                results.add(new SuccinctRegExMatch(range, 1));
              }
            }
            break;
          }
          case CHAR_RANGE: {
            char[] charRange = p.getPrimitiveStr().toCharArray();
            for (char c : charRange) {
              Range range = succinctFile.bwdSearch(String.valueOf(c).getBytes());
              if (!range.empty()) {
                results.add(new SuccinctRegExMatch(range, 1));
              }
            }
            break;
          }
        }
        break;
      }
      case Union: {
        RegExUnion u = (RegExUnion) r;
        TreeSet<SuccinctRegExMatch> firstMatches, secondMatches;
        firstMatches = computeSuccinctly(u.getFirst());
        secondMatches = computeSuccinctly(u.getSecond());
        results = regexUnion(firstMatches, secondMatches);
        break;
      }
      case Concat: {
        RegExConcat c = (RegExConcat) r;
        TreeSet<SuccinctRegExMatch> rightResults = computeSuccinctly(c.getRight());
        for (SuccinctRegExMatch rightMatch : rightResults) {
          TreeSet<SuccinctRegExMatch> temp = regexConcat(c.getLeft(), rightMatch);
          results = regexUnion(results, temp);
        }
        break;
      }
      case Repeat: {
        RegExRepeat rep = (RegExRepeat) r;
        switch (rep.getRegExRepeatType()) {
          case ZeroOrMore: {
            results = regexRepeatOneOrMore(rep.getInternal());
            break;
          }
          case OneOrMore: {
            results = regexRepeatOneOrMore(rep.getInternal());
            break;
          }
          case MinToMax: {
            results = regexRepeatMinToMax(rep.getInternal(), rep.getMin(), rep.getMax());
            break;
          }
        }
        break;
      }
      default: {
        throw new RuntimeException("Invalid node in succinct regex parse tree.");
      }
    }

    return results;
  }

  /**
   * Returns the union of two result sets.
   *
   * @param first  The first result set.
   * @param second The second result set.
   * @return The union of the two result sets.
   */
  private TreeSet<SuccinctRegExMatch> regexUnion(TreeSet<SuccinctRegExMatch> first,
    TreeSet<SuccinctRegExMatch> second) {
    TreeSet<SuccinctRegExMatch> unionResults = new TreeSet<>();
    unionResults.addAll(first);
    unionResults.addAll(second);
    return unionResults;
  }

  /**
   * Performs concatenation of left subtree with a Succinct match.
   *
   * @param r          Right subtree.
   * @param rightMatch Right succinct match.
   * @return Concatenation of left match with right subtree.
   */
  private TreeSet<SuccinctRegExMatch> regexConcat(RegEx r, SuccinctRegExMatch rightMatch) {
    TreeSet<SuccinctRegExMatch> concatResults = new TreeSet<>();

    if (rightMatch.empty()) {
      return concatResults;
    }

    switch (r.getRegExType()) {
      case Blank: {
        break;
      }
      case Primitive: {
        RegExPrimitive p = (RegExPrimitive) r;
        switch (p.getPrimitiveType()) {
          case MGRAM: {
            String mgram = p.getPrimitiveStr();
            Range range = succinctFile.continueBwdSearch(mgram.getBytes(), rightMatch);
            if (!range.empty()) {
              concatResults
                .add(new SuccinctRegExMatch(range, rightMatch.getLength() + mgram.length()));
            }
            break;
          }
          case DOT: {
            for (byte b : alphabet) {
              char c = (char) b;
              if ((b == SuccinctCore.EOL) || (b == SuccinctCore.EOF || (b == SuccinctCore.EOA))) {
                continue;
              }
              Range range =
                succinctFile.continueBwdSearch(String.valueOf(c).getBytes(), rightMatch);
              if (!range.empty()) {
                concatResults.add(new SuccinctRegExMatch(range, rightMatch.getLength() + 1));
              }
            }
            break;
          }
          case CHAR_RANGE: {
            char[] charRange = p.getPrimitiveStr().toCharArray();
            for (char c : charRange) {
              Range range =
                succinctFile.continueBwdSearch(String.valueOf(c).getBytes(), rightMatch);
              if (!range.empty()) {
                concatResults.add(new SuccinctRegExMatch(range, rightMatch.getLength() + 1));
              }
            }
            break;
          }
        }
        break;
      }
      case Union: {
        RegExUnion u = (RegExUnion) r;
        TreeSet<SuccinctRegExMatch> firstResults, secondResults;
        firstResults = regexConcat(u.getFirst(), rightMatch);
        secondResults = regexConcat(u.getSecond(), rightMatch);
        concatResults = regexUnion(firstResults, secondResults);
        break;
      }
      case Concat: {
        RegExConcat c = (RegExConcat) r;
        TreeSet<SuccinctRegExMatch> rightOfLefResults = regexConcat(c.getRight(), rightMatch);
        for (SuccinctRegExMatch rightOfLeftMatch : rightOfLefResults) {
          TreeSet<SuccinctRegExMatch> temp = regexConcat(c.getLeft(), rightOfLeftMatch);
          concatResults = regexUnion(concatResults, temp);
        }
        break;
      }
      case Repeat: {
        RegExRepeat rep = (RegExRepeat) r;
        switch (rep.getRegExRepeatType()) {
          case ZeroOrMore: {
            concatResults = regexRepeatZeroOrMore(rep.getInternal(), rightMatch);
            break;
          }
          case OneOrMore: {
            concatResults = regexRepeatOneOrMore(rep.getInternal(), rightMatch);
            break;
          }
          case MinToMax: {
            concatResults =
              regexRepeatMinToMax(rep.getInternal(), rightMatch, rep.getMin(), rep.getMax());
            break;
          }
        }
        break;
      }
      default: {
        throw new RuntimeException("Invalid node in succinct regex parse tree.");
      }
    }

    return concatResults;
  }

  /**
   * Repeat regular expression one or more times.
   *
   * @param r The regular expression.
   * @return The results for repeat.
   */
  private TreeSet<SuccinctRegExMatch> regexRepeatOneOrMore(RegEx r) {
    TreeSet<SuccinctRegExMatch> repeatResults = new TreeSet<>();
    TreeSet<SuccinctRegExMatch> internalResults = computeSuccinctly(r);
    if (internalResults.isEmpty()) {
      return repeatResults;
    }

    repeatResults.addAll(internalResults);
    for (SuccinctRegExMatch internalMatch : internalResults) {
      repeatResults = regexUnion(repeatResults, regexRepeatOneOrMore(r, internalMatch));
    }
    return repeatResults;
  }

  /**
   * Repeat regular expression one or more times given right match.
   *
   * @param r         The regular expression.
   * @param rightMatch The right match.
   * @return The results for repeat.
   */
  private TreeSet<SuccinctRegExMatch> regexRepeatOneOrMore(RegEx r, SuccinctRegExMatch rightMatch) {
    TreeSet<SuccinctRegExMatch> repeatResults = new TreeSet<>();
    if (rightMatch.empty()) {
      return repeatResults;
    }

    TreeSet<SuccinctRegExMatch> concatResults = regexConcat(r, rightMatch);
    if (concatResults.isEmpty()) {
      return repeatResults;
    }

    repeatResults.addAll(concatResults);
    for (SuccinctRegExMatch concatMatch : concatResults) {
      repeatResults = regexUnion(repeatResults, regexRepeatOneOrMore(r, concatMatch));
    }
    return repeatResults;
  }

  /**
   * Repeat regular expression zero or more times given right match.
   *
   * @param r         The regular expression.
   * @param rightMatch The right match.
   * @return The results for repeat.
   */
  private TreeSet<SuccinctRegExMatch> regexRepeatZeroOrMore(RegEx r, SuccinctRegExMatch rightMatch) {
    TreeSet<SuccinctRegExMatch> repeatResults = new TreeSet<>();
    if (rightMatch.empty()) {
      return repeatResults;
    }

    repeatResults.add(rightMatch);
    repeatResults = regexUnion(repeatResults, regexRepeatOneOrMore(r, rightMatch));
    return repeatResults;
  }

  /**
   * Repeat regular expression from min to max times.
   *
   * @param r   The regular expression.
   * @param min The minimum number of repetitions.
   * @param max The maximum number of repetitions.
   * @return The results for repeat.
   */
  private TreeSet<SuccinctRegExMatch> regexRepeatMinToMax(RegEx r, int min, int max) {
    min = (min > 0) ? min - 1 : 0;
    max = (max > 0) ? max - 1 : 0;

    TreeSet<SuccinctRegExMatch> repeatResults = new TreeSet<>();
    TreeSet<SuccinctRegExMatch> internalResults = computeSuccinctly(r);
    if (internalResults.isEmpty()) {
      return repeatResults;
    }

    if (min == 0) {
      repeatResults.addAll(internalResults);
    }

    if (max > 0) {
      for (SuccinctRegExMatch internalMatch : internalResults) {
        repeatResults = regexUnion(repeatResults, regexRepeatMinToMax(r, internalMatch, min, max));
      }
    }
    return repeatResults;
  }

  /**
   * Repeat the regular expression from min to max times given right match.
   *
   * @param r         The regular expression.
   * @param rightMatch The right match.
   * @param min       The minimum number of repetitions.
   * @param max       The maximum number of repetitions.
   * @return The results for repeat.
   */
  private TreeSet<SuccinctRegExMatch> regexRepeatMinToMax(RegEx r, SuccinctRegExMatch rightMatch,
    int min, int max) {
    min = (min > 0) ? min - 1 : 0;
    max = (max > 0) ? max - 1 : 0;

    TreeSet<SuccinctRegExMatch> repeatResults = new TreeSet<>();
    if (rightMatch.empty()) {
      return repeatResults;
    }

    TreeSet<SuccinctRegExMatch> concatResults = regexConcat(r, rightMatch);
    if (concatResults.isEmpty()) {
      return repeatResults;
    }

    if (min == 0) {
      repeatResults.addAll(concatResults);
    }

    if (max > 0) {
      for (SuccinctRegExMatch concatMatch : concatResults) {
        repeatResults = regexUnion(repeatResults, regexRepeatMinToMax(r, concatMatch, min, max));
      }
    }

    return repeatResults;
  }

}
