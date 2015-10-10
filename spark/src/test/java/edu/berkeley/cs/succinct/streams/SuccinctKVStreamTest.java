package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.buffers.SuccinctKVBuffer;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class SuccinctKVStreamTest extends TestCase {

  private String testFileRaw = this.getClass().getResource("/raw.dat").getFile();
  private String testFileSuccinct =
    this.getClass().getResource("/raw.dat").getFile() + ".idx.succinct";

  private SuccinctKVStream<String> sStream;
  private Map<String, String> kv;
  private int numKeys;
  private Random random;

  private String generateKey() {
    return String.valueOf(Math.abs(random.nextInt(numKeys)));
  }

  public void setUp() throws Exception {
    super.setUp();

    // Create kv map
    int key = 0;
    kv = new TreeMap<String, String>();
    int valueBufferSize = 0;
    try (BufferedReader br = new BufferedReader(new FileReader(testFileRaw))) {
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

    SuccinctKVBuffer<String> sKVBuf =
      new SuccinctKVBuffer<String>(keys, stringBuilder.toString().getBytes(), offsets);
    sKVBuf.writeToFile(testFileSuccinct);

    sStream = new SuccinctKVStream<String>(new Path(testFileSuccinct));
    random = new Random();
  }

  public void testGet() throws Exception {
    System.out.println("get");
    for (int i = 0; i < 1000; i++) {
      String key = generateKey();
      assertTrue(Arrays.equals(kv.get(key).getBytes(), sStream.get(key)));
    }
  }

  public void testDelete() throws Exception {
    System.out.println("delete");
    for (int i = 0; i < 1000; i++) {
      String key = generateKey();
      assertTrue(sStream.delete(key));
      assertNull(sStream.get(key));
    }
  }

  public void testNumEntries() throws Exception {
    System.out.println("numEntries");
    assertEquals(kv.size(), sStream.numEntries());
  }

  public void testKeyIterator() throws Exception {
    System.out.println("keyIterator");
    Iterator<String> expected = kv.keySet().iterator();
    Iterator<String> actual = sStream.keyIterator();
    while (expected.hasNext()) {
      assertTrue(actual.hasNext());
      assertEquals(expected.next(), actual.next());
    }
    assertFalse(actual.hasNext());
  }
}
