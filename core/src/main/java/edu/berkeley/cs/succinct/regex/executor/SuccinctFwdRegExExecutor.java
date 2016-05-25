package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.SuccinctRegExMatch;
import edu.berkeley.cs.succinct.regex.parser.*;
import edu.berkeley.cs.succinct.util.SuccinctConstants;
import edu.berkeley.cs.succinct.util.container.Range;

import java.util.HashSet;

public class SuccinctFwdRegExExecutor extends SuccinctRegExExecutor {

  private int[] alphabet;

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
  @Override protected HashSet<SuccinctRegExMatch> computeSuccinctly(RegEx r) {
    HashSet<SuccinctRegExMatch> results = new HashSet<>();
    switch (r.getRegExType()) {
      case Blank: {
        break;
      }
      case Primitive: {
        RegExPrimitive p = (RegExPrimitive) r;
        switch (p.getPrimitiveType()) {
          case MGRAM: {
            String mgram = p.getPrimitiveStr();
            Range range = succinctFile.fwdSearch(mgram.toCharArray());
            if (!range.empty()) {
              results.add(new SuccinctRegExMatch(range, mgram.length()));
            }
            break;
          }
          case DOT: {
            for (int b : alphabet) {
              char c = (char) b;
              if ((b == SuccinctConstants.EOL) || (b == SuccinctConstants.EOF)) {
                continue;
              }
              Range range = succinctFile.fwdSearch(String.valueOf(c).toCharArray());
              if (!range.empty()) {
                results.add(new SuccinctRegExMatch(range, 1));
              }
            }
            break;
          }
          case CHAR_RANGE: {
            char[] charRange = p.getPrimitiveStr().toCharArray();
            for (char c : charRange) {
              Range range = succinctFile.fwdSearch(String.valueOf(c).toCharArray());
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
        HashSet<SuccinctRegExMatch> firstMatches, secondMatches;
        firstMatches = computeSuccinctly(u.getFirst());
        secondMatches = computeSuccinctly(u.getSecond());
        results = regexUnion(firstMatches, secondMatches);
        break;
      }
      case Concat: {
        RegExConcat c = (RegExConcat) r;
        HashSet<SuccinctRegExMatch> leftResults = computeSuccinctly(c.getLeft());
        for (SuccinctRegExMatch leftMatch : leftResults) {
          HashSet<SuccinctRegExMatch> temp = regexConcat(c.getRight(), leftMatch);
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
  private HashSet<SuccinctRegExMatch> regexUnion(HashSet<SuccinctRegExMatch> first,
    HashSet<SuccinctRegExMatch> second) {
    HashSet<SuccinctRegExMatch> unionResults = new HashSet<>();
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
  private HashSet<SuccinctRegExMatch> regexConcat(RegEx r, SuccinctRegExMatch leftMatch) {
    HashSet<SuccinctRegExMatch> concatResults = new HashSet<>();

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
              succinctFile.continueFwdSearch(mgram.toCharArray(), leftMatch, leftMatch.getLength());
            if (!range.empty()) {
              concatResults
                .add(new SuccinctRegExMatch(range, leftMatch.getLength() + mgram.length()));
            }
            break;
          }
          case DOT: {
            for (int b : alphabet) {
              char c = (char) b;
              if ((b == SuccinctConstants.EOL) || (b == SuccinctConstants.EOF)) {
                continue;
              }
              Range range = succinctFile
                .continueFwdSearch(String.valueOf(c).toCharArray(), leftMatch, leftMatch.getLength());
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
                .continueFwdSearch(String.valueOf(c).toCharArray(), leftMatch, leftMatch.getLength());
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
        HashSet<SuccinctRegExMatch> firstResults, secondResults;
        firstResults = regexConcat(u.getFirst(), leftMatch);
        secondResults = regexConcat(u.getSecond(), leftMatch);
        concatResults = regexUnion(firstResults, secondResults);
        break;
      }
      case Concat: {
        RegExConcat c = (RegExConcat) r;
        HashSet<SuccinctRegExMatch> leftOfRightResults = regexConcat(c.getLeft(), leftMatch);
        for (SuccinctRegExMatch leftOfRightMatch : leftOfRightResults) {
          HashSet<SuccinctRegExMatch> temp = regexConcat(c.getRight(), leftOfRightMatch);
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
  private HashSet<SuccinctRegExMatch> regexRepeatOneOrMore(RegEx r) {
    HashSet<SuccinctRegExMatch> repeatResults = new HashSet<>();
    HashSet<SuccinctRegExMatch> internalResults = computeSuccinctly(r);
    if (internalResults.isEmpty()) {
      return repeatResults;
    }

    repeatResults.addAll(internalResults);
    for (SuccinctRegExMatch internalMatch: internalResults) {
      HashSet<SuccinctRegExMatch> longer = regexRepeatOneOrMore(r, internalMatch);
      repeatResults = regexUnion(repeatResults, longer);
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
  private HashSet<SuccinctRegExMatch> regexRepeatOneOrMore(RegEx r, SuccinctRegExMatch leftMatch) {
    HashSet<SuccinctRegExMatch> repeatResults = new HashSet<>();
    if (leftMatch.empty()) {
      return repeatResults;
    }

    HashSet<SuccinctRegExMatch> concatResults = regexConcat(r, leftMatch);
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
  private HashSet<SuccinctRegExMatch> regexRepeatZeroOrMore(RegEx r, SuccinctRegExMatch leftMatch) {
    HashSet<SuccinctRegExMatch> repeatResults = new HashSet<>();
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
  private HashSet<SuccinctRegExMatch> regexRepeatMinToMax(RegEx r, int min, int max) {
    min = (min > 0) ? min - 1: 0;
    max = (max > 0) ? max - 1: 0;

    HashSet<SuccinctRegExMatch> repeatResults = new HashSet<>();
    HashSet<SuccinctRegExMatch> internalResults = computeSuccinctly(r);
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
  private HashSet<SuccinctRegExMatch> regexRepeatMinToMax(RegEx r, SuccinctRegExMatch leftMatch, int min, int max) {
    min = (min > 0) ? min - 1: 0;
    max = (max > 0) ? max - 1: 0;

    HashSet<SuccinctRegExMatch> repeatResults = new HashSet<>();
    if (leftMatch.empty()) {
      return repeatResults;
    }

    HashSet<SuccinctRegExMatch> concatResults = regexConcat(r, leftMatch);
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
