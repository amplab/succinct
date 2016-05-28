package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import junit.framework.TestCase;

import java.util.Set;

abstract public class RegExExecutorTest extends TestCase {
  protected String input = "YoHoYoHoHoYoYoHoHoHo";
  protected SuccinctFileBuffer succinctFile = new SuccinctFileBuffer(input.getBytes());

  abstract Set<RegExMatch> runRegEx(String exp, boolean greedy)
    throws RegExParsingException;

  public void testExecuteGreedy() throws Exception {

    // Test M-Gram Search
    Set<RegExMatch> r1 = runRegEx("Yo", true);
    assertEquals(4, r1.size());
    assertTrue(r1.contains(new RegExMatch(0, 2)));
    assertTrue(r1.contains(new RegExMatch(4, 2)));
    assertTrue(r1.contains(new RegExMatch(10, 2)));
    assertTrue(r1.contains(new RegExMatch(12, 2)));

    // Test Union
    Set<RegExMatch> r2 = runRegEx("Yo|Ho", true);
    assertEquals(10, r2.size());
    assertTrue(r2.contains(new RegExMatch(0, 2)));
    assertTrue(r2.contains(new RegExMatch(2, 2)));
    assertTrue(r2.contains(new RegExMatch(4, 2)));
    assertTrue(r2.contains(new RegExMatch(6, 2)));
    assertTrue(r2.contains(new RegExMatch(8, 2)));
    assertTrue(r2.contains(new RegExMatch(10, 2)));
    assertTrue(r2.contains(new RegExMatch(12, 2)));
    assertTrue(r2.contains(new RegExMatch(14, 2)));
    assertTrue(r2.contains(new RegExMatch(16, 2)));
    assertTrue(r2.contains(new RegExMatch(18, 2)));

    // Test concat
    Set<RegExMatch> r3 = runRegEx("Yo(Yo|Ho)", true);
    assertEquals(4, r3.size());
    assertTrue(r3.contains(new RegExMatch(0, 4)));
    assertTrue(r3.contains(new RegExMatch(4, 4)));
    assertTrue(r3.contains(new RegExMatch(10, 4)));
    assertTrue(r3.contains(new RegExMatch(12, 4)));

    // Test Repeat
    Set<RegExMatch> r4 = runRegEx("Yo(Ho)+", true);
    assertEquals(3, r4.size());
    assertTrue(r4.contains(new RegExMatch(0, 4)));
    assertTrue(r4.contains(new RegExMatch(4, 6)));
    assertTrue(r4.contains(new RegExMatch(12, 8)));

    Set<RegExMatch> r5 = runRegEx("(Ho)+Yo", true);
    assertEquals(2, r5.size());
    assertTrue(r5.contains(new RegExMatch(2, 4)));
    assertTrue(r5.contains(new RegExMatch(6, 6)));

    Set<RegExMatch> r6 = runRegEx("Ho+", true);
    assertEquals(3, r6.size());
    assertTrue(r6.contains(new RegExMatch(2, 2)));
    assertTrue(r6.contains(new RegExMatch(6, 4)));
    assertTrue(r6.contains(new RegExMatch(14, 6)));

    // Test wildcard
    Set<RegExMatch> r7 = runRegEx("Yo.*Ho", true);
    assertEquals(1, r7.size());
    assertTrue(r7.contains(new RegExMatch(0, 20)));

  }

  public void testExecute() throws Exception {

    // Test M-Gram Search
    Set<RegExMatch> r1 = runRegEx("Yo", false);
    assertEquals(4, r1.size());
    assertTrue(r1.contains(new RegExMatch(0, 2)));
    assertTrue(r1.contains(new RegExMatch(4, 2)));
    assertTrue(r1.contains(new RegExMatch(10, 2)));
    assertTrue(r1.contains(new RegExMatch(12, 2)));

    // Test Union
    Set<RegExMatch> r2 = runRegEx("Yo|Ho", false);
    assertEquals(10, r2.size());
    assertTrue(r2.contains(new RegExMatch(0, 2)));
    assertTrue(r2.contains(new RegExMatch(2, 2)));
    assertTrue(r2.contains(new RegExMatch(4, 2)));
    assertTrue(r2.contains(new RegExMatch(6, 2)));
    assertTrue(r2.contains(new RegExMatch(8, 2)));
    assertTrue(r2.contains(new RegExMatch(10, 2)));
    assertTrue(r2.contains(new RegExMatch(12, 2)));
    assertTrue(r2.contains(new RegExMatch(14, 2)));
    assertTrue(r2.contains(new RegExMatch(16, 2)));
    assertTrue(r2.contains(new RegExMatch(18, 2)));

    // Test concat
    Set<RegExMatch> r3 = runRegEx("Yo(Yo|Ho)", false);
    assertEquals(4, r3.size());
    assertTrue(r3.contains(new RegExMatch(0, 4)));
    assertTrue(r3.contains(new RegExMatch(4, 4)));
    assertTrue(r3.contains(new RegExMatch(10, 4)));
    assertTrue(r3.contains(new RegExMatch(12, 4)));

    // Test Repeat
    Set<RegExMatch> r4 = runRegEx("Yo(Ho)+", false);
    assertEquals(6, r4.size());
    assertTrue(r4.contains(new RegExMatch(0, 4)));
    assertTrue(r4.contains(new RegExMatch(4, 6)));
    assertTrue(r4.contains(new RegExMatch(4, 4)));
    assertTrue(r4.contains(new RegExMatch(12, 4)));
    assertTrue(r4.contains(new RegExMatch(12, 6)));
    assertTrue(r4.contains(new RegExMatch(12, 8)));


    Set<RegExMatch> r5 = runRegEx("(Ho)+Yo", false);
    assertEquals(3, r5.size());
    assertTrue(r5.contains(new RegExMatch(2, 4)));
    assertTrue(r5.contains(new RegExMatch(6, 6)));
    assertTrue(r5.contains(new RegExMatch(8, 4)));

    Set<RegExMatch> r6 = runRegEx("Ho+", false);
    assertEquals(10, r6.size());
    assertTrue(r6.contains(new RegExMatch(2, 2)));
    assertTrue(r6.contains(new RegExMatch(6, 2)));
    assertTrue(r6.contains(new RegExMatch(6, 4)));
    assertTrue(r6.contains(new RegExMatch(8, 2)));
    assertTrue(r6.contains(new RegExMatch(14, 2)));
    assertTrue(r6.contains(new RegExMatch(14, 4)));
    assertTrue(r6.contains(new RegExMatch(14, 6)));
    assertTrue(r6.contains(new RegExMatch(16, 2)));
    assertTrue(r6.contains(new RegExMatch(16, 4)));
    assertTrue(r6.contains(new RegExMatch(18, 2)));

    // Test wildcard
    Set<RegExMatch> r7 = runRegEx("Yo.*Ho", false);
    assertEquals(1, r7.size());
    assertTrue(r7.contains(new RegExMatch(0, 20)));

  }
}
