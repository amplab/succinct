package edu.berkeley.cs.succinct.regex;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.executor.RegExExecutor;
import edu.berkeley.cs.succinct.regex.executor.SuccinctBwdRegExExecutor;
import edu.berkeley.cs.succinct.regex.executor.SuccinctFwdRegExExecutor;
import edu.berkeley.cs.succinct.regex.parser.*;

import java.util.TreeSet;

public class SuccinctRegEx {

  private RegEx regEx;
  private String regExString;
  private SuccinctFile succinctFile;
  private boolean greedy;

  /**
   * Constructor to initialize regular expression.
   *
   * @param succinctFile Input Succinct File.
   * @param regExString  The regular expression as a UTF-8 string.
   * @throws RegExParsingException
   */
  public SuccinctRegEx(SuccinctFile succinctFile, String regExString, boolean greedy)
    throws RegExParsingException {
    this.succinctFile = succinctFile;
    this.regExString = regExString;
    this.regEx = parse();
    this.greedy = greedy;
  }

  /**
   * Constructor to initialize regular expression.
   *
   * @param succinctFile Input Succinct File.
   * @param regExString  The regular expression as a UTF-8 string.
   * @throws RegExParsingException
   */
  public SuccinctRegEx(SuccinctFile succinctFile, String regExString) throws RegExParsingException {
    this(succinctFile, regExString, true);
  }

  /**
   * Parses the regular expression as a tree from the string.
   *
   * @return The regular expression parse tree.
   * @throws RegExParsingException
   */
  private RegEx parse() throws RegExParsingException {
    return (new RegExParser(regExString)).parse();
  }

  /**
   * Check if regular expression has an mgram prefix.
   *
   * @param r The regular expression.
   * @return True if the regular expression has an mgram prefix, false otherwise.
   */
  private boolean isPrefixed(RegEx r) {
    switch (r.getRegExType()) {
      case Blank: {
        return false;
      }
      case Primitive: {
        return ((RegExPrimitive) r).getPrimitiveType() == RegExPrimitive.PrimitiveType.MGRAM;
      }
      case Repeat: {
        return isPrefixed(((RegExRepeat) r).getInternal());
      }
      case Concat: {
        return isPrefixed(((RegExConcat) r).getLeft());
      }
      case Wildcard: {
        return isPrefixed(((RegExWildcard) r).getLeft());
      }
      case Union: {
        return isPrefixed(((RegExUnion) r).getFirst()) && isPrefixed(((RegExUnion) r).getSecond());
      }
    }

    return false;
  }

  /**
   * Check if regular expression has an mgram suffix.
   *
   * @param r The regular expression.
   * @return True if the regular expression has an mgram suffix, false otherwise.
   */
  private boolean isSuffixed(RegEx r) {
    switch (r.getRegExType()) {
      case Blank: {
        return false;
      }
      case Primitive: {
        return ((RegExPrimitive) r).getPrimitiveType() == RegExPrimitive.PrimitiveType.MGRAM;
      }
      case Repeat: {
        return isSuffixed(((RegExRepeat) r).getInternal());
      }
      case Concat: {
        return isSuffixed(((RegExConcat) r).getRight());
      }
      case Wildcard: {
        return isSuffixed(((RegExWildcard) r).getRight());
      }
      case Union: {
        return isSuffixed(((RegExUnion) r).getFirst()) && isSuffixed(((RegExUnion) r).getSecond());
      }
    }

    return false;
  }

  /**
   * Prints a regular expression parse sub-tree.
   *
   * @param re The regular expression parse sub-tree.
   */
  private void printRegEx(RegEx re) {
    switch (re.getRegExType()) {
      case Blank: {
        System.out.print("Blank");
        break;
      }
      case Wildcard:
        RegExWildcard w = (RegExWildcard) re;
        System.out.print("Wildcard(");
        printRegEx(w.getLeft());
        System.out.print(",");
        printRegEx(w.getRight());
        System.out.print(")");
        break;
      case Primitive: {
        RegExPrimitive p = ((RegExPrimitive) re);
        System.out.print("Primitive:" + p.getPrimitiveStr());
        break;
      }
      case Repeat: {
        RegExRepeat r = (RegExRepeat) re;
        System.out.print("Repeat(");
        printRegEx(r.getInternal());
        System.out.print(")");
        break;
      }
      case Concat: {
        RegExConcat co = (RegExConcat) re;
        System.out.print("Concat(");
        printRegEx(co.getLeft());
        System.out.print(",");
        printRegEx(co.getRight());
        System.out.print(")");
        break;
      }
      case Union: {
        RegExUnion u = (RegExUnion) re;
        System.out.print("Union(");
        printRegEx(u.getFirst());
        System.out.print(",");
        printRegEx(u.getSecond());
        System.out.print(")");
        break;
      }
    }
  }

  /**
   * Prints the parsed regular expression tree.
   */
  public void printRegEx() {
    printRegEx(regEx);
  }

  /**
   * Computes the results for the regular expression.
   *
   * @return The results for the regular expression.
   */
  public TreeSet<RegExMatch> compute() {
    RegExExecutor executor;
    if (isSuffixed(regEx) || !isPrefixed(regEx)) {
      executor = new SuccinctBwdRegExExecutor(succinctFile, regEx, greedy);
    } else {
      executor = new SuccinctFwdRegExExecutor(succinctFile, regEx, greedy);
    }
    executor.execute();
    return executor.getFinalResults();
  }
}
