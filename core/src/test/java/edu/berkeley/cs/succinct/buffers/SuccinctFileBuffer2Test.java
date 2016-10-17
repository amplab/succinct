package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.SuccinctFileTest;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.util.Source;

import java.io.*;
import java.util.Arrays;
import java.util.Set;

public class SuccinctFileBuffer2Test extends SuccinctFileTest {

  char[] data;
  private String testFileRaw = this.getClass().getResource("/test_file_utf8").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/test_file_utf8").getFile() + ".buf.succinct";
  private String[] queryStrings =
    {"kΩ", "əsoʊsiˈeıʃn", "‘single’", "გაიაროთ", "в", "ร", "ተ", "ᚻᛖ", "⡌⠁", "╳", "rand"};

  @Override public String getQueryString(int i) {
    return queryStrings[i];
  }

  @Override public int numQueryStrings() {
    return queryStrings.length;
  }

  @Override public String getData() {
    return new String(data);
  }

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();

    File inputFile = new File(testFileRaw);

    data = new char[(int) inputFile.length()];
    InputStreamReader inputReader = new InputStreamReader(new FileInputStream(inputFile), "UTF8");
    inputReader.read(data, 0, data.length);
    fileData = new Source() {
      @Override public int length() {
        return data.length;
      }

      @Override public int get(int i) {
        return data[i];
      }
    };

    sFile = new SuccinctFileBuffer(data);
  }

  public void testSerializeDeserialize() throws Exception {

    // Serialize data
    FileOutputStream fOut = new FileOutputStream(testFileSuccinct);
    ObjectOutputStream oos = new ObjectOutputStream(fOut);
    oos.writeObject(sFile);
    oos.close();

    // Deserialize data
    FileInputStream fIn = new FileInputStream(testFileSuccinct);
    ObjectInputStream ois = new ObjectInputStream(fIn);
    SuccinctFile sFileRead = (SuccinctFileBuffer) ois.readObject();
    ois.close();

    assertNotNull(sFileRead);
    assertEquals(sFile.getSize(), sFileRead.getSize());
    assertTrue(Arrays.equals(sFile.extractBytes(0, sFile.getSize()),
      sFileRead.extractBytes(0, sFileRead.getSize())));
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
