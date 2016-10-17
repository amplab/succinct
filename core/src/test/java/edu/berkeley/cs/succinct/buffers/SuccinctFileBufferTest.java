package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.SuccinctFileTest;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.util.Source;

import java.io.*;
import java.util.Arrays;
import java.util.Set;

public class SuccinctFileBufferTest extends SuccinctFileTest {

  byte[] data;
  private String testFileRaw = this.getClass().getResource("/test_file").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/test_file").getFile() + ".buf.succinct";
  private String[] queryStrings = {"int", "include", "random", "random int"};

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

    data = new byte[(int) inputFile.length()];
    DataInputStream dis = new DataInputStream(new FileInputStream(inputFile));
    dis.readFully(data);
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
