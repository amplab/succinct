package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.regex.RegExMatch;
import junit.framework.TestCase;

import java.util.Iterator;
import java.util.Set;

abstract public class SuccinctFileTest extends TestCase {

  protected SuccinctFile sFile;
  protected byte[] fileData;

  /**
   * Test method: char charAt(long i)
   *
   * @throws Exception
   */
  public void testCharAt() throws Exception {

    for (int i = 0; i < sFile.getSize() - 1; i++) {
      char c = sFile.charAt(i);
      assertEquals(fileData[i], c);
    }
  }

  /**
   * Test method: byte[] extract(int offset, int len)
   *
   * @throws Exception
   */
  public void testExtract() throws Exception {

    byte[] buf1 = sFile.extract(0, 100);
    assertEquals(100, buf1.length);
    for (int i = 0; i < 100; i++) {
      assertEquals(fileData[i], buf1[i]);
    }

    byte[] buf2 = sFile.extract(fileData.length - 101, 100);
    assertEquals(100, buf2.length);
    for (int i = 0; i < 100; i++) {
      assertEquals(fileData[fileData.length - 101 + i], buf2[i]);
    }
  }

  /**
   * Test method: byte[] extractUntil(int offset, char delim)
   *
   * @throws Exception
   */
  public void testExtractUntil() throws Exception {

    byte[] buf = sFile.extractUntil(0, (byte) '\n');
    for (int i = 0; i < buf.length; i++) {
      assertEquals(fileData[i], buf[i]);
      assertFalse(buf[i] == '\n');
    }
    assertEquals(fileData[buf.length], '\n');
  }

  /**
   * Test method: long count(byte[] query)
   *
   * @throws Exception
   */
  public void testCount() throws Exception {

    long count1 = sFile.count("int".getBytes());
    assertEquals(43, count1);

    long count2 = sFile.count("include".getBytes());
    assertEquals(9, count2);

    long count3 = sFile.count("random".getBytes());
    assertEquals(0, count3);

    long count4 = sFile.count("random int".getBytes());
    assertEquals(0, count4);
  }

  /**
   * Test method: long[] search(byte[] query)
   *
   * @throws Exception
   */
  public void testSearch() throws Exception {

    byte[] query1 = "int".getBytes();
    Long[] positions1 = sFile.search(query1);
    assertEquals(43, positions1.length);
    for (Long aPositions1 : positions1) {
      for (int j = 0; j < query1.length; j++) {
        assertEquals(query1[j], fileData[((int) (aPositions1 + j))]);
      }
    }

    byte[] query2 = "include".getBytes();
    Long[] positions2 = sFile.search(query2);
    assertEquals(9, positions2.length);
    for (Long aPositions2 : positions2) {
      for (int j = 0; j < query2.length; j++) {
        assertEquals(query2[j], fileData[((int) (aPositions2 + j))]);
      }
    }

    byte[] query3 = "random".getBytes();
    Long[] positions3 = sFile.search(query3);
    assertEquals(0, positions3.length);

    byte[] query4 = "random int".getBytes();
    Long[] positions4 = sFile.search(query4);
    assertEquals(0, positions4.length);

  }

  /**
   * Test method: Iterator<Long> searchIterator(byte[] query)
   *
   * @throws Exception
   */
  public void testSearchIterator() throws Exception {

    byte[] query1 = "int".getBytes();
    Iterator<Long> positions1 = sFile.searchIterator(query1);
    long count1 = 0;
    while (positions1.hasNext()) {
      long position1 = positions1.next();
      for (int j = 0; j < query1.length; j++) {
        assertEquals(query1[j], fileData[((int) (position1 + j))]);
      }
      count1++;
    }
    assertEquals(43, count1);

    byte[] query2 = "include".getBytes();
    Iterator<Long> positions2 = sFile.searchIterator(query2);
    long count2 = 0;
    while (positions2.hasNext()) {
      long position2 = positions2.next();
      for (int j = 0; j < query2.length; j++) {
        assertEquals(query2[j], fileData[((int) (position2 + j))]);
      }
      count2++;
    }
    assertEquals(9, count2);

    byte[] query3 = "random".getBytes();
    Iterator<Long> positions3 = sFile.searchIterator(query3);
    assertFalse(positions3.hasNext());

    byte[] query4 = "random int".getBytes();
    Iterator<Long> positions4 = sFile.searchIterator(query4);
    assertFalse(positions4.hasNext());

  }

  /**
   * Helper method to check results for a regex query.
   *
   * @param results Results to check.
   * @param exp     Expression to check against.
   * @return The check result.
   */
  private boolean checkResults(Set<RegExMatch> results, String exp) {
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
   * @param results Results to check.
   * @param exp1    First expression to check against.
   * @param exp2    Second expression to check against.
   * @return The check result.
   */
  private boolean checkResultsUnion(Set<RegExMatch> results, String exp1, String exp2) {
    for (RegExMatch m : results) {
      boolean flagFirst = true;
      boolean flagSecond = true;
      for (int i = 0; i < exp1.length(); i++) {
        if (fileData[(int) (m.getOffset() + i)] != exp1.charAt(i)) {
          flagFirst = false;
        }
      }

      for (int i = 0; i < exp2.length(); i++) {
        if (fileData[(int) (m.getOffset() + i)] != exp2.charAt(i)) {
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
   * @param results Results to check.
   * @param exp     Expression to check against.
   * @return The check result.
   */
  private boolean checkResultsRepeat(Set<RegExMatch> results, String exp) {
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
   * Test method: Map<Long, Integer> regexSearch(String query)
   *
   * @throws Exception
   */
  public void testRegexSearch() throws Exception {

    Set<RegExMatch> primitiveResults1 = sFile.regexSearch("c");
    assertTrue(checkResults(primitiveResults1, "c"));

    Set<RegExMatch> primitiveResults2 = sFile.regexSearch("in");
    assertTrue(checkResults(primitiveResults2, "in"));

    Set<RegExMatch> primitiveResults3 = sFile.regexSearch("out");
    assertTrue(checkResults(primitiveResults3, "out"));

    Set<RegExMatch> unionResults = sFile.regexSearch("in|out");
    assertTrue(checkResultsUnion(unionResults, "in", "out"));

    Set<RegExMatch> concatResults = sFile.regexSearch("c(in|out)");
    assertTrue(checkResultsUnion(concatResults, "cin", "cout"));

    Set<RegExMatch> repeatResults = sFile.regexSearch("c+");
    assertTrue(checkResultsRepeat(repeatResults, "c"));
  }
}
