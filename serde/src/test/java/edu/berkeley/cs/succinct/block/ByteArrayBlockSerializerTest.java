package edu.berkeley.cs.succinct.block;

import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Iterator;

public class ByteArrayBlockSerializerTest extends TestCase {

  public void testSerialize() throws Exception {
    byte[][] data = new byte[][]{"apple".getBytes(), "banana".getBytes(), "carrot".getBytes()};
    Iterator<byte[]> dataIterator = Arrays.asList(data).iterator();
    byte recordSeparator = -125;
    ByteArrayBlockSerializer serializer = new ByteArrayBlockSerializer(recordSeparator);
    BlockSerializer.SerializedData serializedData = serializer.serialize(dataIterator);

    // Check offsets
    int[] offsets = serializedData.getOffsets();
    assertEquals(3, offsets.length);
    int curOffset = 1;
    for (int i = 0; i < offsets.length; i++) {
      assertEquals(offsets[i], curOffset);
      curOffset += (data[i].length + 1);
    }

    // Check metadata
    assertEquals(null, serializedData.getMetadata());

    // Check serialized data
    byte[] expectedSerializedBytes = new byte[] {recordSeparator, 'a', 'p', 'p', 'l', 'e',
      recordSeparator, 'b', 'a', 'n', 'a', 'n', 'a', recordSeparator, 'c', 'a', 'r', 'r', 'o', 't',
      recordSeparator};

    assertTrue(Arrays.equals(expectedSerializedBytes, serializedData.getData()));
  }
}
