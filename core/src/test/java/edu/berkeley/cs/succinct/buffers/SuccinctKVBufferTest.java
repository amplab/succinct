package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import junit.framework.TestCase;

import java.io.*;
import java.util.*;

public class SuccinctKVBufferTest extends TestCase {

  private String testFileRaw = this.getClass().getResource("/test_file").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/test_file").getFile() + ".idx.succinct";
  private String testFileSuccinctMin =
    this.getClass().getResource("/test_file").getFile() + ".idx.min.succinct";

  private SuccinctKVBuffer<String> sKVBuf;
  private Map<String, String> kv;
  private int numKeys;
  private Random random;

  private String generateKey() {
    return String.valueOf(Math.abs(random.nextInt(numKeys)));
  }

  public void setUp() throws Exception {
    super.setUp();

    File inputFile = new File(testFileRaw);

    // Create kv map
    int key = 0;
    kv = new TreeMap<String, String>();
    int valueBufferSize = 0;
    try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
      String line;
      while ((line = br.readLine()) != null) {
        kv.put(String.valueOf(key), line);
        valueBufferSize += (line.length() + 1);
        key++;
      }
    }

    numKeys = key;

    // Convert to Succinct representation
    String[] keys = new String[numKeys];
    int[] offsets = new int[numKeys];
    StringBuilder stringBuilder = new StringBuilder(valueBufferSize + 1);
    int i = 0, curOffset = 0;
    for (Map.Entry<String, String> entry : kv.entrySet()) {
      keys[i] = entry.getKey();
      offsets[i] = curOffset;
      String value = entry.getValue();
      stringBuilder.append(value);
      stringBuilder.append('\n');
      curOffset += (value.length() + 1);
      i++;
    }

    sKVBuf = new SuccinctKVBuffer<String>(keys, stringBuilder.toString().getBytes(), offsets);
    random = new Random();
  }

  public void testGet() throws Exception {
    System.out.println("get");
    for (int i = 0; i < 1000; i++) {
      String key = generateKey();
      assertTrue(Arrays.equals(kv.get(key).getBytes(), sKVBuf.get(key)));
    }
  }

  public void testDelete() throws Exception {
    System.out.println("delete");
    for (int i = 0; i < 1000; i++) {
      String key = generateKey();
      assertTrue(sKVBuf.delete(key));
      assertNull(sKVBuf.get(key));
    }
  }

  public void testNumEntries() throws Exception {
    System.out.println("numEntries");
    assertEquals(kv.size(), sKVBuf.numEntries());
  }

  public void testKeyIterator() throws Exception {
    System.out.println("keyIterator");
    Iterator<String> expected = kv.keySet().iterator();
    Iterator<String> actual = sKVBuf.keyIterator();
    while (expected.hasNext()) {
      assertTrue(actual.hasNext());
      assertEquals(expected.next(), actual.next());
    }
    assertFalse(actual.hasNext());
  }

  /**
   * Test method: void readObject(ObjectInputStream ois)
   * Test method: void writeObject(ObjectOutputStream oos)
   *
   * @throws Exception
   */
  public void testSerializeDeserialize() throws Exception {
    System.out.println("serializeDeserialize");

    // Serialize data
    FileOutputStream fOut = new FileOutputStream(testFileSuccinct);
    ObjectOutputStream oos = new ObjectOutputStream(fOut);
    oos.writeObject(sKVBuf);
    oos.close();

    // Deserialize data
    FileInputStream fIn = new FileInputStream(testFileSuccinct);
    ObjectInputStream ois = new ObjectInputStream(fIn);
    SuccinctKVBuffer<String> sKVBufRead = (SuccinctKVBuffer<String>) ois.readObject();
    ois.close();

    assertNotNull(sKVBufRead);
    for (int i = 0; i < numKeys; i++) {
      String key = String.valueOf(i);
      assertTrue(Arrays.equals(kv.get(key).getBytes(), sKVBufRead.get(key)));
    }
  }

  /**
   * Test method: void writeToFile(String path)
   * Test method: void readFromFile(String path)
   *
   * @throws Exception
   */
  public void testReadFromFile() throws Exception {
    System.out.println("readFromFile");

    sKVBuf.writeToFile(testFileSuccinctMin);
    SuccinctKVBuffer<String> sKVBufRead =
      new SuccinctKVBuffer<String>(testFileSuccinctMin, StorageMode.MEMORY_ONLY);

    assertNotNull(sKVBufRead);
    for (int i = 0; i < numKeys; i++) {
      String key = String.valueOf(i);
      assertTrue(Arrays.equals(kv.get(key).getBytes(), sKVBufRead.get(key)));
    }
  }
}
