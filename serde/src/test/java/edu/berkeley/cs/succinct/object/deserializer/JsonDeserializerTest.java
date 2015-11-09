package edu.berkeley.cs.succinct.object.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.berkeley.cs.succinct.DataType;
import edu.berkeley.cs.succinct.block.json.FieldMapping;
import junit.framework.TestCase;

import java.io.IOException;

public class JsonDeserializerTest extends TestCase {

  static ObjectMapper mapper = new ObjectMapper();

  static void assertJsonEquals(String json1, String json2) throws IOException {
    JsonNode tree1 = mapper.readTree(json1);
    JsonNode tree2 = mapper.readTree(json2);
    assertTrue(tree1.equals(tree2));
  }

  public void testDeserialize() throws Exception {
    byte[] jsonSerializedData1 = new byte[] {-120, 'a', 'b', 'c', -120, -119, '1', '8', -119};
    byte[] jsonSerializedData2 = new byte[] {-118, 'a', 'b', 'c', -118, -117, 'd', 'e', 'f', -117,
      -119, '1', '6', '.', '5', -119};

    String expectedJsonString1 = "{\"name\":\"abc\", \"age\":18.0}";
    String expectedJsonString2 = "{\"name\": {\"first\":\"abc\", \"last\":\"def\"}, \"age\":16.5}";

    FieldMapping mapping = new FieldMapping();
    mapping.put("name", (byte) -120, DataType.STRING);
    mapping.put("age", (byte) -119, DataType.FLOAT);
    mapping.put("name.first", (byte) -118, DataType.STRING);
    mapping.put("name.last", (byte) -117, DataType.STRING);

    JsonDeserializer deserializer = new JsonDeserializer(mapping);
    String jsonString1 = deserializer.deserialize(jsonSerializedData1);
    assertJsonEquals(expectedJsonString1, jsonString1);

    String jsonString2 = deserializer.deserialize(jsonSerializedData2);
    assertJsonEquals(expectedJsonString2, jsonString2);
  }
}
