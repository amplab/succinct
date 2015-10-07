package edu.berkeley.cs.succinct.regex.planner;

import edu.berkeley.cs.succinct.regex.parser.RegEx;
import edu.berkeley.cs.succinct.regex.parser.RegExPrimitive;
import junit.framework.TestCase;

public class NaiveRegExPlannerTest extends TestCase {

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test method: RegEx plan()
   *
   * @throws Exception
   */
  public void testPlan() throws Exception {
    System.out.println("plan");

    RegEx regEx = new RegExPrimitive("");
    NaiveRegExPlanner planner = new NaiveRegExPlanner(null, regEx);

    assertEquals(planner.plan(), regEx);
  }
}
