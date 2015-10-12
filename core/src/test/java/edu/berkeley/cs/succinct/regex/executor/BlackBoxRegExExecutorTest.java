package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.regex.parser.*;
import junit.framework.TestCase;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Random;
import java.util.TreeSet;

public class BlackBoxRegExExecutorTest extends TestCase {

  private String testFileRaw = this.getClass().getResource("/test_file").getFile();
  private BlackBoxRegExExecutor blackBoxRegExExecutor;
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
  private RegExMatch generateMatch() {
    return new RegExMatch(Math.abs(generator.nextInt(1000)), Math.abs(generator.nextInt(99) + 1));
  }

  /**
   * Helper method to check results for a regex query.
   *
   * @param regEx Input regular expression.
   * @param exp   Expression to check against.
   * @return The check result.
   */
  private boolean checkResults(RegEx regEx, String exp) {
    TreeSet<RegExMatch> results;

    blackBoxRegExExecutor = new BlackBoxRegExExecutor(sBuf, regEx);
    blackBoxRegExExecutor.execute();
    results = blackBoxRegExExecutor.getFinalResults();
    for (RegExMatch m : results) {
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
    TreeSet<RegExMatch> results;

    blackBoxRegExExecutor = new BlackBoxRegExExecutor(sBuf, regEx);
    blackBoxRegExExecutor.execute();
    results = blackBoxRegExExecutor.getFinalResults();
    for (RegExMatch m : results) {
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
    TreeSet<RegExMatch> results;

    blackBoxRegExExecutor = new BlackBoxRegExExecutor(sBuf, regEx);
    blackBoxRegExExecutor.execute();
    results = blackBoxRegExExecutor.getFinalResults();
    for (RegExMatch m : results) {
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

    RegEx primitive1 = new RegExPrimitive("c", RegExPrimitive.PrimitiveType.MGRAM);
    RegEx primitive2 = new RegExPrimitive("out", RegExPrimitive.PrimitiveType.MGRAM);
    RegEx primitive3 = new RegExPrimitive("in", RegExPrimitive.PrimitiveType.MGRAM);
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
    RegExPrimitive regEx = new RegExPrimitive(query, RegExPrimitive.PrimitiveType.MGRAM);
    blackBoxRegExExecutor = new BlackBoxRegExExecutor(sBuf, regEx);
    TreeSet<RegExMatch> results =
      blackBoxRegExExecutor.mgramSearch(regEx, BlackBoxRegExExecutor.SortType.FRONT_SORTED);

    for (RegExMatch m : results) {
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
    TreeSet<RegExMatch> A = new TreeSet<RegExMatch>(RegExMatch.FRONT_COMPARATOR);
    TreeSet<RegExMatch> B = new TreeSet<RegExMatch>(RegExMatch.FRONT_COMPARATOR);
    for (int i = 0; i < 100; i++) {
      A.add(generateMatch());
      B.add(generateMatch());
    }

    // Check if union is correct
    blackBoxRegExExecutor = new BlackBoxRegExExecutor(null, null);
    TreeSet<RegExMatch> unionRes =
      blackBoxRegExExecutor.regexUnion(A, B, BlackBoxRegExExecutor.SortType.FRONT_SORTED);

    for (RegExMatch m : A) {
      assertTrue(unionRes.contains(m));
    }

    for (RegExMatch m : B) {
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
  private TreeSet<RegExMatch> naiveConcat(TreeSet<RegExMatch> left, TreeSet<RegExMatch> right) {
    TreeSet<RegExMatch> res = new TreeSet<RegExMatch>();
    for (RegExMatch m1 : left) {
      for (RegExMatch m2 : right) {
        if (m1.getOffset() + m1.getLength() == m2.getOffset()) {
          res.add(new RegExMatch(m1.getOffset(), m1.getLength() + m2.getLength()));
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
    TreeSet<RegExMatch> A = new TreeSet<RegExMatch>(RegExMatch.END_COMPARATOR);
    TreeSet<RegExMatch> B = new TreeSet<RegExMatch>(RegExMatch.FRONT_COMPARATOR);
    for (int i = 0; i < 100; i++) {
      A.add(generateMatch());
      B.add(generateMatch());
    }

    // Check if concat is correct
    blackBoxRegExExecutor = new BlackBoxRegExExecutor(null, null);
    TreeSet<RegExMatch> concatRes =
      blackBoxRegExExecutor.regexConcat(A, B, BlackBoxRegExExecutor.SortType.FRONT_SORTED);

    TreeSet<RegExMatch> expectedConcatRes = naiveConcat(A, B);

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
    TreeSet<RegExMatch> A = new TreeSet<RegExMatch>(RegExMatch.END_COMPARATOR);
    for (int i = 0; i < 100; i++) {
      RegExMatch m = generateMatch();
      A.add(m);
    }

    // Check if concat is correct
    blackBoxRegExExecutor = new BlackBoxRegExExecutor(null, null);
    TreeSet<RegExMatch> repeatRes =
      blackBoxRegExExecutor.regexRepeat(A, RegExRepeatType.OneOrMore, BlackBoxRegExExecutor.SortType.FRONT_SORTED);

    TreeSet<RegExMatch> expectedRepeatRes = new TreeSet<RegExMatch>(RegExMatch.FRONT_COMPARATOR);
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
    TreeSet<RegExMatch> A = new TreeSet<RegExMatch>(RegExMatch.END_COMPARATOR);
    TreeSet<RegExMatch> B = new TreeSet<RegExMatch>(RegExMatch.FRONT_COMPARATOR);

    for (int i = 0; i < 10; i++) {
      A.add(generateMatch());
      B.add(generateMatch());
    }

    // Check if concat is correct
    blackBoxRegExExecutor = new BlackBoxRegExExecutor(null, null);
    TreeSet<RegExMatch> wildcardRes =
      blackBoxRegExExecutor.regexWildcard(A, B, BlackBoxRegExExecutor.SortType.FRONT_SORTED);

    for (RegExMatch m1 : A) {
      RegExMatch lowerBound = new RegExMatch(m1.end(), 0);
      RegExMatch m2 = B.ceiling(lowerBound);
      if (m2 != null) {
        long distance = m2.begin() - m1.begin();
        RegExMatch entry = new RegExMatch(m1.begin(), (int) (distance + m2.getLength()));
        assertTrue(wildcardRes.contains(entry));
      }
    }
  }
}
