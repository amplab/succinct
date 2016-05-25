package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.SuccinctRegExMatch;
import edu.berkeley.cs.succinct.regex.parser.*;
import edu.berkeley.cs.succinct.util.SuccinctConstants;
import edu.berkeley.cs.succinct.util.container.Range;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

public class SuccinctBwdRegExExecutor extends SuccinctRegExExecutor {

  private int[] alphabet;

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
            Range range = succinctFile.bwdSearch(mgram.toCharArray());
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
              Range range = succinctFile.bwdSearch(String.valueOf(c).toCharArray());
              if (!range.empty()) {
                results.add(new SuccinctRegExMatch(range, 1));
              }
            }
            break;
          }
          case CHAR_RANGE: {
            char[] charRange = p.getPrimitiveStr().toCharArray();
            for (char c : charRange) {
              Range range = succinctFile.bwdSearch(String.valueOf(c).toCharArray());
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
        HashSet<SuccinctRegExMatch> rightResults = computeSuccinctly(c.getRight());
        for (SuccinctRegExMatch rightMatch : rightResults) {
          HashSet<SuccinctRegExMatch> temp = regexConcat(c.getLeft(), rightMatch);
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
   * Performs concatenation of left subtree with a Succinct match.
   *
   * @param r          Right subtree.
   * @param rightMatch Right succinct match.
   * @return Concatenation of left match with right subtree.
   */
  private HashSet<SuccinctRegExMatch> regexConcat(RegEx r, SuccinctRegExMatch rightMatch) {
    HashSet<SuccinctRegExMatch> concatResults = new HashSet<>();

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
            Range range = succinctFile.continueBwdSearch(mgram.toCharArray(), rightMatch);
            if (!range.empty()) {
              concatResults
                .add(new SuccinctRegExMatch(range, rightMatch.getLength() + mgram.length()));
            }
            break;
          }
          case DOT: {
            for (int b : alphabet) {
              char c = (char) b;
              if ((b == SuccinctConstants.EOL) || (b == SuccinctConstants.EOF)) {
                continue;
              }
              Range range =
                succinctFile.continueBwdSearch(String.valueOf(c).toCharArray(), rightMatch);
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
                succinctFile.continueBwdSearch(String.valueOf(c).toCharArray(), rightMatch);
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
        HashSet<SuccinctRegExMatch> firstResults, secondResults;
        firstResults = regexConcat(u.getFirst(), rightMatch);
        secondResults = regexConcat(u.getSecond(), rightMatch);
        concatResults = regexUnion(firstResults, secondResults);
        break;
      }
      case Concat: {
        RegExConcat c = (RegExConcat) r;
        HashSet<SuccinctRegExMatch> rightOfLefResults = regexConcat(c.getRight(), rightMatch);
        for (SuccinctRegExMatch rightOfLeftMatch : rightOfLefResults) {
          HashSet<SuccinctRegExMatch> temp = regexConcat(c.getLeft(), rightOfLeftMatch);
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
   * Computes the seed to repeat with no suffix.
   *
   * @param r The repeat operand.
   * @return Initial set of results to continue repetitions.
   */
  private HashSet<SuccinctRegExMatch> computeSeedToRepeat(RegEx r) {
    HashSet<SuccinctRegExMatch> results = computeSuccinctly(r);

    TreeSet<SuccinctRegExMatch> initialRepeats = new TreeSet<>(new Comparator<SuccinctRegExMatch>() {
      @Override public int compare(SuccinctRegExMatch o1, SuccinctRegExMatch o2) {
        return (int) (o1.begin() - o2.begin());
      }
    });

    for (SuccinctRegExMatch result: results) {
      initialRepeats.addAll(regexConcat(r, result));
    }

    Iterator<SuccinctRegExMatch> it = initialRepeats.iterator();
    if (!it.hasNext()) {
      return results;
    }

    SuccinctRegExMatch first = it.next();
    long start = first.begin();
    long end = first.end();

    while (it.hasNext()) {
      SuccinctRegExMatch current = it.next();
      if (current.begin() <= end) {
        end = Math.max(current.end(), end);
      } else {
        // Remove subrange.
        HashSet<SuccinctRegExMatch> newSubRanges = new HashSet<>();
        for (Iterator<SuccinctRegExMatch> i = results.iterator(); i.hasNext();) {
          SuccinctRegExMatch match = i.next();
          if (match.contains(start, end)) {
            i.remove();
            if (match.begin() == start && match.end() != end) {
              newSubRanges.add(new SuccinctRegExMatch(end + 1, match.end(), match.getLength()));
            } else if (match.end() == end && match.begin() != start) {
              newSubRanges.add(new SuccinctRegExMatch(match.begin(), start - 1, match.getLength()));
            } else if (match.begin() != start && match.end() != end) {
              newSubRanges.add(new SuccinctRegExMatch(match.begin(), start - 1, match.getLength()));
              newSubRanges.add(new SuccinctRegExMatch(end + 1, match.end(), match.getLength()));
            }
          }
        }
        results.addAll(newSubRanges);

        start = current.begin();
        end = current.end();
      }
    }

    // Remove subrange.
    HashSet<SuccinctRegExMatch> newSubRanges = new HashSet<>();
    for (Iterator<SuccinctRegExMatch> i = results.iterator(); i.hasNext();) {
      SuccinctRegExMatch match = i.next();
      if (match.contains(start, end)) {
        i.remove();
        if (match.begin() == start && match.end() != end) {
          newSubRanges.add(new SuccinctRegExMatch(end + 1, match.end(), match.getLength()));
        } else if (match.end() == end && match.begin() != start) {
          newSubRanges.add(new SuccinctRegExMatch(match.begin(), start - 1, match.getLength()));
        } else if (match.begin() != start && match.end() != end) {
          newSubRanges.add(new SuccinctRegExMatch(match.begin(), start - 1, match.getLength()));
          newSubRanges.add(new SuccinctRegExMatch(end + 1, match.end(), match.getLength()));
        }
      }
    }
    results.addAll(newSubRanges);

    return results;
  }

  /**
   * Repeat regular expression one or more times.
   *
   * @param r The regular expression.
   * @return The results for repeat.
   */
  private HashSet<SuccinctRegExMatch> regexRepeatOneOrMore(RegEx r) {
    HashSet<SuccinctRegExMatch> repeatResults = new HashSet<>();
    HashSet<SuccinctRegExMatch> internalResults = computeSeedToRepeat(r);
    if (internalResults.isEmpty()) {
      return repeatResults;
    }

    repeatResults.addAll(internalResults);
    for (SuccinctRegExMatch internalMatch : internalResults) {
      HashSet<SuccinctRegExMatch> moreRepeats = regexRepeatOneOrMore(r, internalMatch);
      repeatResults = regexUnion(repeatResults, moreRepeats);
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
  private HashSet<SuccinctRegExMatch> regexRepeatOneOrMore(RegEx r, SuccinctRegExMatch rightMatch) {
    HashSet<SuccinctRegExMatch> repeatResults = new HashSet<>();
    if (rightMatch.empty()) {
      return repeatResults;
    }

    HashSet<SuccinctRegExMatch> concatResults = regexConcat(r, rightMatch);
    if (concatResults.isEmpty()) {
      return repeatResults;
    }

    repeatResults.addAll(concatResults);
    for (SuccinctRegExMatch concatMatch : concatResults) {
      HashSet<SuccinctRegExMatch> moreRepeats = regexRepeatOneOrMore(r, concatMatch);
      repeatResults = regexUnion(repeatResults, moreRepeats);
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
  private HashSet<SuccinctRegExMatch> regexRepeatZeroOrMore(RegEx r, SuccinctRegExMatch rightMatch) {
    HashSet<SuccinctRegExMatch> repeatResults = new HashSet<>();
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
  private HashSet<SuccinctRegExMatch> regexRepeatMinToMax(RegEx r, int min, int max) {
    min = (min > 0) ? min - 1 : 0;
    max = (max > 0) ? max - 1 : 0;

    HashSet<SuccinctRegExMatch> repeatResults = new HashSet<>();
    HashSet<SuccinctRegExMatch> internalResults = computeSeedToRepeat(r);
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
  private HashSet<SuccinctRegExMatch> regexRepeatMinToMax(RegEx r, SuccinctRegExMatch rightMatch,
    int min, int max) {
    min = (min > 0) ? min - 1 : 0;
    max = (max > 0) ? max - 1 : 0;

    HashSet<SuccinctRegExMatch> repeatResults = new HashSet<>();
    if (rightMatch.empty()) {
      return repeatResults;
    }

    HashSet<SuccinctRegExMatch> concatResults = regexConcat(r, rightMatch);
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
