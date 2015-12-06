package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import junit.framework.TestCase;

import java.util.Set;

abstract public class RegExExecutorTest extends TestCase {
  protected String input = "YoHoYoHoHoYoYoHoHoHo";
  protected SuccinctFileBuffer succinctFile = new SuccinctFileBuffer(input.getBytes());

  abstract Set<RegExMatch> runRegEx(String exp)
    throws RegExParsingException;

  public void testExecute() throws Exception {

    // Test M-Gram Search
    Set<RegExMatch> r1 = runRegEx("Yo");
    assertEquals(4, r1.size());
    assertTrue(r1.contains(new RegExMatch(0, 2)));
    assertTrue(r1.contains(new RegExMatch(4, 2)));
    assertTrue(r1.contains(new RegExMatch(10, 2)));
    assertTrue(r1.contains(new RegExMatch(12, 2)));

    // Test Union
    Set<RegExMatch> r2 = runRegEx("Yo|Ho");
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
    Set<RegExMatch> r3 = runRegEx("Yo(Yo|Ho)");
    assertEquals(4, r3.size());
    assertTrue(r3.contains(new RegExMatch(0, 4)));
    assertTrue(r3.contains(new RegExMatch(4, 4)));
    assertTrue(r3.contains(new RegExMatch(10, 4)));
    assertTrue(r3.contains(new RegExMatch(12, 4)));

    // Test Repeat
    Set<RegExMatch> r4 = runRegEx("Ho+");
    assertEquals(10, r4.size());
    assertTrue(r4.contains(new RegExMatch(2, 2)));
    assertTrue(r4.contains(new RegExMatch(6, 2)));
    assertTrue(r4.contains(new RegExMatch(6, 4)));
    assertTrue(r4.contains(new RegExMatch(8, 2)));
    assertTrue(r4.contains(new RegExMatch(14, 2)));
    assertTrue(r4.contains(new RegExMatch(14, 4)));
    assertTrue(r4.contains(new RegExMatch(14, 6)));
    assertTrue(r4.contains(new RegExMatch(16, 2)));
    assertTrue(r4.contains(new RegExMatch(16, 4)));
    assertTrue(r4.contains(new RegExMatch(18, 2)));

    // Test wildcard
    Set<RegExMatch> r5 = runRegEx("Yo.*Ho");
    assertEquals(4, r5.size());
    assertTrue(r5.contains(new RegExMatch(0, 20)));
    assertTrue(r5.contains(new RegExMatch(4, 16)));
    assertTrue(r5.contains(new RegExMatch(10, 10)));
    assertTrue(r5.contains(new RegExMatch(12, 8)));

  }
}
