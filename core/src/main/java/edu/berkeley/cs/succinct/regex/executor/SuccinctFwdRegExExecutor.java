package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.SuccinctCore;
import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.SuccinctRegExMatch;
import edu.berkeley.cs.succinct.regex.parser.*;
import edu.berkeley.cs.succinct.util.container.Range;

import java.util.TreeSet;

public class SuccinctFwdRegExExecutor extends SuccinctRegExExecutor {

  private byte[] alphabet;

  /**
   * Constructor to initialize SuccinctFwdRegExExecutor.
   *
   * @param succinctFile The input SuccinctCore.
   * @param regEx        The input regular expression.
   */
  public SuccinctFwdRegExExecutor(SuccinctFile succinctFile, RegEx regEx) {
    super(succinctFile, regEx);
    this.alphabet = succinctFile.getAlphabet();
  }

  /**
   * Uses Succinct data representation (forward search) to compute regular expression.
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
            Range range = succinctFile.fwdSearch(mgram.getBytes());
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
              Range range = succinctFile.fwdSearch(String.valueOf(c).getBytes());
              if (!range.empty()) {
                results.add(new SuccinctRegExMatch(range, 1));
              }
            }
            break;
          }
          case CHAR_RANGE: {
            char[] charRange = p.getPrimitiveStr().toCharArray();
            for (char c : charRange) {
              Range range = succinctFile.fwdSearch(String.valueOf(c).getBytes());
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
        TreeSet<SuccinctRegExMatch> leftResults = computeSuccinctly(c.getLeft());
        for (SuccinctRegExMatch leftMatch : leftResults) {
          TreeSet<SuccinctRegExMatch> temp = regexConcat(c.getRight(), leftMatch);
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
   * Performs concatenation of a Succinct match with right subtree.
   *
   * @param r Right subtree.
   * @param leftMatch Left succinct match.
   * @return Concatenation of left match with right subtree.
   */
  private TreeSet<SuccinctRegExMatch> regexConcat(RegEx r, SuccinctRegExMatch leftMatch) {
    TreeSet<SuccinctRegExMatch> concatResults = new TreeSet<>();

    if (leftMatch.empty()) {
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
            Range range =
              succinctFile.continueFwdSearch(mgram.getBytes(), leftMatch, leftMatch.getLength());
            if (!range.empty()) {
              concatResults
                .add(new SuccinctRegExMatch(range, leftMatch.getLength() + mgram.length()));
            }
            break;
          }
          case DOT: {
            for (byte b : alphabet) {
              char c = (char) b;
              if ((b == SuccinctCore.EOL) || (b == SuccinctCore.EOF || (b == SuccinctCore.EOA))) {
                continue;
              }
              Range range = succinctFile
                .continueFwdSearch(String.valueOf(c).getBytes(), leftMatch, leftMatch.getLength());
              if (!range.empty()) {
                concatResults.add(new SuccinctRegExMatch(range, leftMatch.getLength() + 1));
              }
            }
            break;
          }
          case CHAR_RANGE: {
            char[] charRange = p.getPrimitiveStr().toCharArray();
            for (char c : charRange) {
              Range range = succinctFile
                .continueFwdSearch(String.valueOf(c).getBytes(), leftMatch, leftMatch.getLength());
              if (!range.empty()) {
                concatResults.add(new SuccinctRegExMatch(range, leftMatch.getLength() + 1));
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
        firstResults = regexConcat(u.getFirst(), leftMatch);
        secondResults = regexConcat(u.getSecond(), leftMatch);
        concatResults = regexUnion(firstResults, secondResults);
        break;
      }
      case Concat: {
        RegExConcat c = (RegExConcat) r;
        TreeSet<SuccinctRegExMatch> leftOfRightResults = regexConcat(c.getLeft(), leftMatch);
        for (SuccinctRegExMatch leftOfRightMatch : leftOfRightResults) {
          TreeSet<SuccinctRegExMatch> temp = regexConcat(c.getRight(), leftOfRightMatch);
          concatResults = regexUnion(concatResults, temp);
        }
        break;
      }
      case Repeat: {
        RegExRepeat rep = (RegExRepeat) r;
        switch (rep.getRegExRepeatType()) {
          case ZeroOrMore: {
            concatResults = regexRepeatZeroOrMore(rep.getInternal(), leftMatch);
            break;
          }
          case OneOrMore: {
            concatResults = regexRepeatOneOrMore(rep.getInternal(), leftMatch);
            break;
          }
          case MinToMax: {
            concatResults = regexRepeatMinToMax(rep.getInternal(), leftMatch, rep.getMin(), rep.getMax());
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
    for (SuccinctRegExMatch internalMatch: internalResults) {
      repeatResults = regexUnion(repeatResults, regexRepeatOneOrMore(r, internalMatch));
    }
    return repeatResults;
  }

  /**
   * Repeat regular expression one or more times given left match.
   *
   * @param r The regular expression.
   * @param leftMatch The left match.
   * @return The results for repeat.
   */
  private TreeSet<SuccinctRegExMatch> regexRepeatOneOrMore(RegEx r, SuccinctRegExMatch leftMatch) {
    TreeSet<SuccinctRegExMatch> repeatResults = new TreeSet<>();
    if (leftMatch.empty()) {
      return repeatResults;
    }

    TreeSet<SuccinctRegExMatch> concatResults = regexConcat(r, leftMatch);
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
   * Repeat regular expression zero or more times given left match.
   *
   * @param r The regular expression.
   * @param leftMatch The left match.
   * @return The results for repeat.
   */
  private TreeSet<SuccinctRegExMatch> regexRepeatZeroOrMore(RegEx r, SuccinctRegExMatch leftMatch) {
    TreeSet<SuccinctRegExMatch> repeatResults = new TreeSet<>();
    if (leftMatch.empty()) {
      return repeatResults;
    }

    repeatResults.add(leftMatch);
    repeatResults = regexUnion(repeatResults, regexRepeatOneOrMore(r, leftMatch));
    return repeatResults;
  }

  /**
   * Repeat regular expression from min to max times.
   *
   * @param r The regular expression.
   * @param min The minimum number of repetitions.
   * @param max The maximum number of repetitions.
   * @return The results for repeat.
   */
  private TreeSet<SuccinctRegExMatch> regexRepeatMinToMax(RegEx r, int min, int max) {
    min = (min > 0) ? min - 1: 0;
    max = (max > 0) ? max - 1: 0;

    TreeSet<SuccinctRegExMatch> repeatResults = new TreeSet<>();
    TreeSet<SuccinctRegExMatch> internalResults = computeSuccinctly(r);
    if (internalResults.isEmpty()) {
      return repeatResults;
    }

    if (min == 0) {
      repeatResults.addAll(internalResults);
    }

    if (max > 0) {
      for (SuccinctRegExMatch internalMatch: internalResults) {
        repeatResults = regexUnion(repeatResults, regexRepeatMinToMax(r, internalMatch, min, max));
      }
    }
    return repeatResults;
  }

  /**
   * Repeat the regular expression from min to max times given left match.
   *
   * @param r The regular expression.
   * @param leftMatch The left match.
   * @param min The minimum number of repetitions.
   * @param max The maximum number of repetitions.
   * @return The results for repeat.
   */
  private TreeSet<SuccinctRegExMatch> regexRepeatMinToMax(RegEx r, SuccinctRegExMatch leftMatch, int min, int max) {
    min = (min > 0) ? min - 1: 0;
    max = (max > 0) ? max - 1: 0;

    TreeSet<SuccinctRegExMatch> repeatResults = new TreeSet<>();
    if (leftMatch.empty()) {
      return repeatResults;
    }

    TreeSet<SuccinctRegExMatch> concatResults = regexConcat(r, leftMatch);
    if (concatResults.isEmpty()) {
      return repeatResults;
    }

    if (min == 0) {
      repeatResults.addAll(concatResults);
    }

    if (max > 0) {
      for (SuccinctRegExMatch concatMatch: concatResults) {
        repeatResults = regexUnion(repeatResults, regexRepeatMinToMax(r, concatMatch, min, max));
      }
    }

    return repeatResults;
  }

}
