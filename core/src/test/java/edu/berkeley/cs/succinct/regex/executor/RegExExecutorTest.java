package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.regex.RegexMatch;
import edu.berkeley.cs.succinct.regex.parser.*;
import junit.framework.TestCase;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Random;
import java.util.TreeSet;

public class RegExExecutorTest extends TestCase {

  private String testFileRaw = this.getClass().getResource("/test_file").getFile();
  private RegExExecutor regExExecutor;
  private SuccinctFileBuffer sBuf;
  private byte[] fileData;
  private Random generator;

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
    File inputFile = new File(testFileRaw);

    fileData = new byte[(int) inputFile.length()];
    DataInputStream dis = new DataInputStream(new FileInputStream(inputFile));
    dis.readFully(fileData);
    sBuf = new SuccinctFileBuffer(fileData);
    generator = new Random();
  }

  /**
   * Generates a random regex match.
   *
   * @return A random regex match.
   */
  private RegexMatch generateMatch() {
    return new RegexMatch(Math.abs(generator.nextInt(1000)), Math.abs(generator.nextInt(99) + 1));
  }

  /**
   * Helper method to check results for a regex query.
   *
   * @param regEx Input regular expression.
   * @param exp   Expression to check against.
   * @return The check result.
   */
  private boolean checkResults(RegEx regEx, String exp) {
    TreeSet<RegexMatch> results;

    regExExecutor = new RegExExecutor(sBuf, regEx);
    regExExecutor.execute();
    results = regExExecutor.getFinalResults();
    for (RegexMatch m : results) {
      for (int i = 0; i < exp.length(); i++) {
        if (fileData[((int) (m.getOffset() + i))] != exp.charAt(i)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Helper method to check results for a union query.
   *
   * @param regEx Input regular expression.
   * @param exp1  First expression to check against.
   * @param exp2  Second expression to check against.
   * @return The check result.
   */
  private boolean checkResultsUnion(RegEx regEx, String exp1, String exp2) {
    TreeSet<RegexMatch> results;

    regExExecutor = new RegExExecutor(sBuf, regEx);
    regExExecutor.execute();
    results = regExExecutor.getFinalResults();
    for (RegexMatch m : results) {
      boolean flagFirst = true;
      boolean flagSecond = true;
      for (int i = 0; i < exp1.length(); i++) {
        if (fileData[((int) (m.getOffset() + i))] != exp1.charAt(i)) {
          flagFirst = false;
        }
      }

      for (int i = 0; i < exp2.length(); i++) {
        if (fileData[((int) (m.getOffset() + i))] != exp2.charAt(i)) {
          flagSecond = false;
        }
      }

      if (!flagFirst && !flagSecond)
        return false;
    }
    return true;
  }

  /**
   * Helper method to check results for a regex repeat query.
   *
   * @param regEx Input regular expression.
   * @param exp   Expression to check against.
   * @return The check result.
   */
  private boolean checkResultsRepeat(RegEx regEx, String exp) {
    TreeSet<RegexMatch> results;

    regExExecutor = new RegExExecutor(sBuf, regEx);
    regExExecutor.execute();
    results = regExExecutor.getFinalResults();
    for (RegexMatch m : results) {
      for (int i = 0; i < m.getLength(); i++) {
        if (fileData[((int) (m.getOffset() + i))] != exp.charAt(i % exp.length())) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Test method: void execute()
   *
   * @throws Exception
   */
  public void testExecute() throws Exception {
    System.out.println("execute");

    RegEx primitive1 = new RegExPrimitive("c");
    RegEx primitive2 = new RegExPrimitive("out");
    RegEx primitive3 = new RegExPrimitive("in");
    RegEx union = new RegExUnion(primitive2, primitive3);
    RegEx concat = new RegExConcat(primitive1, primitive2);
    RegEx repeat = new RegExRepeat(primitive1, RegExRepeatType.OneOrMore);

    // Check primitives
    assertTrue(checkResults(primitive1, "c"));
    assertTrue(checkResults(primitive2, "out"));
    assertTrue(checkResults(primitive3, "in"));

    // Check operators
    assertTrue(checkResultsUnion(union, "in", "out"));
    assertTrue(checkResults(concat, "cout"));
    assertTrue(checkResultsRepeat(repeat, "c"));
  }

  /**
   * Test method: Map<Long, Integer> mgramSearch(RegExPrimitive rp)
   *
   * @throws Exception
   */
  public void testMgramSearch() throws Exception {
    System.out.println("mgramSearch");

    String query = "int";
    RegExPrimitive regEx = new RegExPrimitive(query);
    regExExecutor = new RegExExecutor(sBuf, regEx);
    TreeSet<RegexMatch> results =
      regExExecutor.mgramSearch(regEx, RegExExecutor.SortType.FRONT_SORTED);

    for (RegexMatch m : results) {
      int len = m.getLength();
      for (int i = 0; i < len; i++) {
        assertEquals(fileData[((int) (m.getOffset() + i))], query.charAt(i));
      }
    }

  }

  /**
   * Test method: TreeSet<RegexMatch> regexUnion(TreeSet<RegexMatch> a, TreeSet<RegexMatch> b, SortType sortType)
   *
   * @throws Exception
   */
  public void testRegexUnion() throws Exception {
    System.out.println("regexUnion");

    // Populate two maps randomly
    TreeSet<RegexMatch> A = new TreeSet<RegexMatch>(RegexMatch.FRONT_COMPARATOR);
    TreeSet<RegexMatch> B = new TreeSet<RegexMatch>(RegexMatch.FRONT_COMPARATOR);
    for (int i = 0; i < 100; i++) {
      A.add(generateMatch());
      B.add(generateMatch());
    }

    // Check if union is correct
    regExExecutor = new RegExExecutor(null, null);
    TreeSet<RegexMatch> unionRes =
      regExExecutor.regexUnion(A, B, RegExExecutor.SortType.FRONT_SORTED);

    for (RegexMatch m : A) {
      assertTrue(unionRes.contains(m));
    }

    for (RegexMatch m : B) {
      assertTrue(unionRes.contains(m));
    }
  }

  /**
   * Computes concat of two result sets using naive concat algorithm.
   *
   * @param left  Left result set.
   * @param right Right result set.
   * @return Concatenation result set.
   */
  private TreeSet<RegexMatch> naiveConcat(TreeSet<RegexMatch> left, TreeSet<RegexMatch> right) {
    TreeSet<RegexMatch> res = new TreeSet<RegexMatch>();
    for (RegexMatch m1 : left) {
      for (RegexMatch m2 : right) {
        if (m1.getOffset() + m1.getLength() == m2.getOffset()) {
          res.add(new RegexMatch(m1.getOffset(), m1.getLength() + m2.getLength()));
        }
      }
    }
    return res;
  }

  /**
   * Test method: TreeSet<RegexMatch> regexConcat(TreeSet<RegexMatch> a, TreeSet<RegexMatch> b, SortType sortType)
   *
   * @throws Exception
   */
  public void testRegexConcat() throws Exception {
    System.out.println("regexConcat");

    // Populate two maps randomly
    TreeSet<RegexMatch> A = new TreeSet<RegexMatch>(RegexMatch.END_COMPARATOR);
    TreeSet<RegexMatch> B = new TreeSet<RegexMatch>(RegexMatch.FRONT_COMPARATOR);
    for (int i = 0; i < 100; i++) {
      A.add(generateMatch());
      B.add(generateMatch());
    }

    // Check if concat is correct
    regExExecutor = new RegExExecutor(null, null);
    TreeSet<RegexMatch> concatRes =
      regExExecutor.regexConcat(A, B, RegExExecutor.SortType.FRONT_SORTED);

    TreeSet<RegexMatch> expectedConcatRes = naiveConcat(A, B);

    assertTrue(concatRes.containsAll(expectedConcatRes));
  }

  /**
   * Test method: TreeSet<RegexMatch> regexRepeat(TreeSet<RegexMatch> a, RegExRepeatType repeatType, SortType sortType)
   *
   * @throws Exception
   */
  public void testRegexRepeat() throws Exception {
    System.out.println("regexRepeat");

    // Populate a map randomly
    TreeSet<RegexMatch> A = new TreeSet<RegexMatch>(RegexMatch.END_COMPARATOR);
    for (int i = 0; i < 100; i++) {
      RegexMatch m = generateMatch();
      A.add(m);
    }

    // Check if concat is correct
    regExExecutor = new RegExExecutor(null, null);
    TreeSet<RegexMatch> repeatRes =
      regExExecutor.regexRepeat(A, RegExRepeatType.OneOrMore, RegExExecutor.SortType.FRONT_SORTED);

    TreeSet<RegexMatch> expectedRepeatRes = new TreeSet<RegexMatch>(RegexMatch.FRONT_COMPARATOR);
    expectedRepeatRes.addAll(A);

    for (int i = 0; i < A.size(); i++) {
      expectedRepeatRes.addAll(naiveConcat(expectedRepeatRes, A));
    }

    assertTrue(repeatRes.containsAll(expectedRepeatRes));
  }

  /**
   * Test method: TreeSet<RegexMatch> regexWildcard(TreeSet<RegexMatch> a, TreeSet<RegexMatch> b, SortType sortType)
   *
   * @throws Exception
   */
  public void testRegexWildcard() throws Exception {
    System.out.println("regexWildcard");

    // Populate two maps randomly
    TreeSet<RegexMatch> A = new TreeSet<RegexMatch>(RegexMatch.END_COMPARATOR);
    TreeSet<RegexMatch> B = new TreeSet<RegexMatch>(RegexMatch.FRONT_COMPARATOR);

    for (int i = 0; i < 10; i++) {
      A.add(generateMatch());
      B.add(generateMatch());
    }

    // Check if concat is correct
    regExExecutor = new RegExExecutor(null, null);
    TreeSet<RegexMatch> wildcardRes =
      regExExecutor.regexWildcard(A, B, RegExExecutor.SortType.FRONT_SORTED);

    for (RegexMatch m1 : A) {
      for (RegexMatch m2 : B) {
        if (m2.after(m1)) {
          long distance = (m2.getOffset() - m1.getOffset());
          RegexMatch m = new RegexMatch(m1.getOffset(), (int) (distance + m2.getLength()));
          assertTrue(wildcardRes.contains(m));
        }
      }
    }

  }
}
