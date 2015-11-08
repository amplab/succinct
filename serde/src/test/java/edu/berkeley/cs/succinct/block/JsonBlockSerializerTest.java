package edu.berkeley.cs.succinct.block;

import edu.berkeley.cs.succinct.block.json.FieldMapping;
import edu.berkeley.cs.succinct.block.json.JsonBlockSerializer;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Iterator;

public class JsonBlockSerializerTest extends TestCase {

  public void testSerialize() throws Exception {

    String jsonString1 = "{\"name\":\"abc\", \"age\":18}";
    String jsonString2 = "{\"name\": { \"first\" : \"abc\", \"last\" : \"def\" }, \"age\":18}";
    String[] jsonData = new String[] {jsonString1, jsonString2};

    byte[] delimiters = new byte[120];
    for (int i = 0; i < 120; i++) {
      delimiters[i] = (byte) (i - 120);
    }

    JsonBlockSerializer serializer = new JsonBlockSerializer(delimiters);
    Iterator<String> jsonDataIterator = Arrays.asList(jsonData).iterator();
    BlockSerializer.SerializedData serializedData = serializer.serialize(jsonDataIterator);

    // Check offsets
    int[] offsets = serializedData.getOffsets();
    assertEquals(2, offsets.length);
    assertEquals(0, offsets[0]);
    assertEquals(9, offsets[1]);

    // Check metadata
    @SuppressWarnings("unchecked")
    FieldMapping fieldMapping = (FieldMapping) serializedData.getMetadata();
    assertEquals(4, fieldMapping.size());
    assertTrue(fieldMapping.containsField("name"));
    assertTrue(fieldMapping.containsField("age"));
    assertTrue(fieldMapping.containsField("name.first"));
    assertTrue(fieldMapping.containsField("name.last"));
    assertEquals(-120, fieldMapping.getDelimiter("name"));
    assertEquals(-119, fieldMapping.getDelimiter("age"));
    assertEquals(-118, fieldMapping.getDelimiter("name.first"));
    assertEquals(-117, fieldMapping.getDelimiter("name.last"));

    // Check serialized data
    byte[] expectedSerializedData = new byte[]{-120, 'a', 'b', 'c', -120, -119, '1', '8', -119,
      -118, 'a', 'b', 'c', -118, -117, 'd', 'e', 'f', -117, -119, '1', '8', -119};
    assertTrue(Arrays.equals(expectedSerializedData, serializedData.getData()));

  }
}
