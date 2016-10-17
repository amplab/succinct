package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctIndexedFile;
import edu.berkeley.cs.succinct.SuccinctTable;
import edu.berkeley.cs.succinct.SuccinctTableTest;
import edu.berkeley.cs.succinct.util.Source;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class SuccinctTableBufferTest extends SuccinctTableTest {

  byte[] data;
  private String testFileRaw = this.getClass().getResource("/raw.dat").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/raw.dat").getFile() + ".idx.succinct";
  private String testFileSuccinctMin =
    this.getClass().getResource("/raw.dat").getFile() + ".idx.min.succinct";

  /**
   * Set up test.
   *
   * @throws Exception
   */
  public void setUp() throws Exception {
    super.setUp();

    queryTypes = new SuccinctTable.QueryType[2];
    queries = new byte[2][][];
    queryTypes[0] = SuccinctTable.QueryType.RangeSearch;
    queries[0] = new byte[][] {"/*".getBytes(), "//".getBytes()};
    queryTypes[1] = SuccinctTable.QueryType.Search;
    queries[1] = new byte[][] {"Build".getBytes()};

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

    ArrayList<Integer> positions = new ArrayList<>();
    positions.add(0);
    for (int i = 0; i < fileData.length(); i++) {
      if (fileData.get(i) == '\n') {
        positions.add(i + 1);
      }
    }
    offsets = new int[positions.size()];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = positions.get(i);
    }
    sTable = new SuccinctTableBuffer(data, offsets);
  }

  /**
   * Test method: void readObject(ObjectInputStream ois)
   * Test method: void writeObject(ObjectOutputStream oos)
   *
   * @throws Exception
   */
  public void testSerializeDeserialize() throws Exception {

    // Serialize data
    FileOutputStream fOut = new FileOutputStream(testFileSuccinct);
    ObjectOutputStream oos = new ObjectOutputStream(fOut);
    oos.writeObject(sTable);
    oos.close();

    // Deserialize data
    FileInputStream fIn = new FileInputStream(testFileSuccinct);
    ObjectInputStream ois = new ObjectInputStream(fIn);
    SuccinctIndexedFile sIFileRead = (SuccinctIndexedFileBuffer) ois.readObject();
    ois.close();

    assertNotNull(sIFileRead);
    assertEquals(sTable.getNumRecords(), sIFileRead.getNumRecords());
    for (int i = 0; i < sTable.getNumRecords(); i++) {
      assertTrue(Arrays.equals(sTable.getRecordBytes(i), sIFileRead.getRecordBytes(i)));
    }
  }

  /**
   * Test method: void writeToFile(String path)
   * Test method: void memoryMap(String path)
   *
   * @throws Exception
   */
  public void testMemoryMap() throws Exception {

    ((SuccinctTableBuffer) sTable).writeToFile(testFileSuccinctMin);
    SuccinctIndexedFile sIFileRead =
      new SuccinctIndexedFileBuffer(testFileSuccinctMin, StorageMode.MEMORY_MAPPED);

    assertNotNull(sIFileRead);
    assertEquals(sTable.getNumRecords(), sIFileRead.getNumRecords());
    for (int i = 0; i < sTable.getNumRecords(); i++) {
      assertTrue(Arrays.equals(sTable.getRecordBytes(i), sIFileRead.getRecordBytes(i)));
    }
  }

  /**
   * Test method: void writeToFile(String path)
   * Test method: void readFromFile(String path)
   *
   * @throws Exception
   */
  public void testReadFromFile() throws Exception {

    ((SuccinctTableBuffer) sTable).writeToFile(testFileSuccinctMin);
    SuccinctIndexedFile sIFileRead =
      new SuccinctIndexedFileBuffer(testFileSuccinctMin, StorageMode.MEMORY_ONLY);

    assertNotNull(sIFileRead);
    assertEquals(sTable.getNumRecords(), sIFileRead.getNumRecords());
    for (int i = 0; i < sTable.getNumRecords(); i++) {
      assertTrue(Arrays.equals(sTable.getRecordBytes(i), sIFileRead.getRecordBytes(i)));
    }
  }
}
