package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.util.Source;
import junit.framework.TestCase;

import java.util.Iterator;
import java.util.Set;

import static edu.berkeley.cs.succinct.TestUtils.stringCount;

abstract public class SuccinctFileTest extends TestCase {

  protected SuccinctFile sFile;
  protected Source fileData;

  abstract public String getQueryString(int i);
  abstract public int numQueryStrings();
  abstract public String getData();

  /**
   * Test method: char charAt(long i)
   *
   * @throws Exception
   */
  public void testCharAt() throws Exception {

    for (int i = 0; i < sFile.getSize() - 1; i++) {
      char c = sFile.charAt(i);
      assertEquals(fileData.get(i), c);
    }
  }

  /**
   * Test method: byte[] extract(int offset, int len)
   *
   * @throws Exception
   */
  public void testExtract() throws Exception {

    String buf1 = sFile.extract(0, 100);
    assertEquals(100, buf1.length());
    for (int i = 0; i < 100; i++) {
      assertEquals(fileData.get(i), buf1.charAt(i));
    }

    String buf2 = sFile.extract(fileData.length() - 101, 100);
    assertEquals(100, buf2.length());
    for (int i = 0; i < 100; i++) {
      assertEquals(fileData.get(fileData.length() - 101 + i), buf2.charAt(i));
    }
  }

  /**
   * Test method: byte[] extractUntil(int offset, char delim)
   *
   * @throws Exception
   */
  public void testExtractBytesUntil() throws Exception {

    String buf = sFile.extractUntil(0, '\n');
    for (int i = 0; i < buf.length(); i++) {
      assertEquals(fileData.get(i), buf.charAt(i));
      assertFalse(buf.charAt(i) == '\n');
    }
    assertEquals(fileData.get(buf.length()), '\n');
  }

  /**
   * Test method: long count(byte[] query)
   *
   * @throws Exception
   */
  public void testCount() throws Exception {

    for (int i = 0; i < numQueryStrings(); i++) {
      String query = getQueryString(i);
      long count = sFile.count(query.toCharArray());
      long expected = stringCount(getData(), query);
      assertEquals(expected, count);
    }

  }

  /**
   * Test method: long[] search(byte[] query)
   *
   * @throws Exception
   */
  public void testSearch() throws Exception {
    for (int i = 0; i < numQueryStrings(); i++) {
      String query = getQueryString(i);
      Long[] positions = sFile.search(query.toCharArray());
      assertEquals(stringCount(getData(), query), positions.length);
      for (Long pos : positions) {
        for (int j = 0; j < query.length(); j++) {
          assertEquals(query.charAt(j), fileData.get((int) (pos + j)));
        }
      }
    }
  }

  /**
   * Test method: Iterator<Long> searchIterator(byte[] query)
   *
   * @throws Exception
   */
  public void testSearchIterator() throws Exception {

    for (int i = 0; i < numQueryStrings(); i++) {
      String query = getQueryString(i);
      Iterator<Long> positions = sFile.searchIterator(query.toCharArray());
      long count = 0;
      while (positions.hasNext()) {
        long pos = positions.next();
        for (int j = 0; j < query.length(); j++) {
          assertEquals(query.charAt(j), fileData.get((int) (pos + j)));
        }
        count++;
      }
      assertEquals(stringCount(getData(), query), count);
    }
  }

  /**
   * Helper method to check results for a regex query.
   *
   * @param results Results to check.
   * @param exp     Expression to check against.
   * @return The check result.
   */
  protected boolean checkResults(Set<RegExMatch> results, String exp) {
    for (RegExMatch m : results) {
      for (int i = 0; i < exp.length(); i++) {
        if (fileData.get(((int) (m.getOffset() + i))) != exp.charAt(i)) {
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
  protected boolean checkResultsUnion(Set<RegExMatch> results, String exp1, String exp2) {
    for (RegExMatch m : results) {
      boolean flagFirst = true;
      boolean flagSecond = true;
      for (int i = 0; i < exp1.length(); i++) {
        if (fileData.get((int) (m.getOffset() + i)) != exp1.charAt(i)) {
          flagFirst = false;
        }
      }

      for (int i = 0; i < exp2.length(); i++) {
        if (fileData.get((int) (m.getOffset() + i)) != exp2.charAt(i)) {
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
  protected boolean checkResultsRepeat(Set<RegExMatch> results, String exp) {
    for (RegExMatch m : results) {
      for (int i = 0; i < m.getLength(); i++) {
        if (fileData.get(((int) (m.getOffset() + i))) != exp.charAt(i % exp.length())) {
          return false;
        }
      }
    }
    return true;
  }
}
