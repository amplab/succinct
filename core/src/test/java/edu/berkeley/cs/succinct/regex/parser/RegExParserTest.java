package edu.berkeley.cs.succinct.regex.parser;

import junit.framework.TestCase;

public class RegExParserTest extends TestCase {

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test method: RegEx parse()
   *
   * @throws Exception
   */
  public void testParse() throws Exception {

    // Parse a simple multi-gram
    RegExParser mgramParser1 = new RegExParser("abcd");
    RegEx mgramRegEx1 = mgramParser1.parse();
    assertEquals(mgramRegEx1.getRegExType(), RegExType.Primitive);
    assertEquals(((RegExPrimitive) mgramRegEx1).getPrimitiveStr(), "abcd");

    // Parse multi-grams with escape characters
    RegExParser mgramParser2 = new RegExParser("\\|a\\(b\\)c\\*d\\+\\{\\}");
    RegEx mgramRegEx2 = mgramParser2.parse();
    assertEquals(mgramRegEx2.getRegExType(), RegExType.Primitive);
    assertEquals(((RegExPrimitive) mgramRegEx2).getPrimitiveStr(), "|a(b)c*d+{}");

    // Parse union
    RegExParser unionParser = new RegExParser("a|b");
    RegEx unionRegEx = unionParser.parse();
    assertEquals(unionRegEx.getRegExType(), RegExType.Union);
    RegExUnion uRE = (RegExUnion) unionRegEx;
    assertEquals(uRE.getFirst().getRegExType(), RegExType.Primitive);
    assertEquals(((RegExPrimitive) uRE.getFirst()).getPrimitiveStr(), "a");
    assertEquals(uRE.getSecond().getRegExType(), RegExType.Primitive);
    assertEquals(((RegExPrimitive) uRE.getSecond()).getPrimitiveStr(), "b");

    // Parse concat
    RegExParser concatParser1 = new RegExParser("a(b|c)");
    RegEx concatRegEx1 = concatParser1.parse();
    assertEquals(concatRegEx1.getRegExType(), RegExType.Concat);
    RegExConcat cRE1 = (RegExConcat) concatRegEx1;
    assertEquals(cRE1.getLeft().getRegExType(), RegExType.Primitive);
    assertEquals(((RegExPrimitive) cRE1.getLeft()).getPrimitiveStr(), "a");
    assertEquals(cRE1.getRight().getRegExType(), RegExType.Union);

    // Parse concat with optimization
    RegExParser concatParser2 = new RegExParser("a(b)");
    RegEx concatRegEx2 = concatParser2.parse();
    assertEquals(concatRegEx2.getRegExType(), RegExType.Primitive);
    assertEquals(((RegExPrimitive) concatRegEx2).getPrimitiveStr(), "ab");

    // Parse repeat: zero or more
    RegExParser repeatParser1 = new RegExParser("a*");
    RegEx repeatRegEx1 = repeatParser1.parse();
    assertEquals(repeatRegEx1.getRegExType(), RegExType.Repeat);
    RegExRepeat rRE1 = (RegExRepeat) repeatRegEx1;
    assertEquals(rRE1.getRegExRepeatType(), RegExRepeatType.ZeroOrMore);
    assertEquals(rRE1.getInternal().getRegExType(), RegExType.Primitive);
    assertEquals(((RegExPrimitive) rRE1.getInternal()).getPrimitiveStr(), "a");

    // Parse repeat: one or more
    RegExParser repeatParser2 = new RegExParser("a+");
    RegEx repeatRegEx2 = repeatParser2.parse();
    assertEquals(repeatRegEx2.getRegExType(), RegExType.Repeat);
    RegExRepeat rRE2 = (RegExRepeat) repeatRegEx2;
    assertEquals(rRE2.getRegExRepeatType(), RegExRepeatType.OneOrMore);
    assertEquals(rRE2.getInternal().getRegExType(), RegExType.Primitive);
    assertEquals(((RegExPrimitive) rRE2.getInternal()).getPrimitiveStr(), "a");

    // Parse repeat: min to max
    RegExParser repeatParser3 = new RegExParser("a{1,2}");
    RegEx repeatRegEx3 = repeatParser3.parse();
    assertEquals(repeatRegEx3.getRegExType(), RegExType.Repeat);
    RegExRepeat rRE3 = (RegExRepeat) repeatRegEx3;
    assertEquals(rRE3.getRegExRepeatType(), RegExRepeatType.MinToMax);
    assertEquals(rRE3.getInternal().getRegExType(), RegExType.Primitive);
    assertEquals(((RegExPrimitive) rRE3.getInternal()).getPrimitiveStr(), "a");
    assertEquals(rRE3.getMin(), 1);
    assertEquals(rRE3.getMax(), 2);

    // Parse wildcard in the beginning and end
    RegExParser wildcardParser1 = new RegExParser(".*abc.*");
    RegEx wildcardRegEx1 = wildcardParser1.parse();
    assertEquals(RegExType.Primitive, wildcardRegEx1.getRegExType());
    RegExPrimitive pRE1 = (RegExPrimitive) wildcardRegEx1;
    assertEquals("abc", pRE1.getPrimitiveStr());
    assertEquals(RegExPrimitive.PrimitiveType.MGRAM, pRE1.getPrimitiveType());

    // Parse wild card in the middle
    RegExParser wildcardParser2 = new RegExParser("a.*b");
    RegEx wildcardRegEx2 = wildcardParser2.parse();
    assertEquals(RegExType.Wildcard, wildcardRegEx2.getRegExType());
    RegExWildcard wRE1 = (RegExWildcard) wildcardRegEx2;
    assertEquals(RegExType.Primitive, wRE1.getLeft().getRegExType());
    assertEquals("a", ((RegExPrimitive) wRE1.getLeft()).getPrimitiveStr());
    assertEquals(RegExType.Primitive, wRE1.getRight().getRegExType());
    assertEquals("b", ((RegExPrimitive) wRE1.getRight()).getPrimitiveStr());

  }
}
