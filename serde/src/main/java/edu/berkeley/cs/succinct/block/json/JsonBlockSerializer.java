package edu.berkeley.cs.succinct.block.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import edu.berkeley.cs.succinct.DataType;
import edu.berkeley.cs.succinct.SerializationException;
import edu.berkeley.cs.succinct.block.BlockSerializer;
import gnu.trove.list.array.TIntArrayList;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class JsonBlockSerializer implements BlockSerializer<String> {

  private ObjectMapper objectMapper;
  private FieldMapping fieldMapping;
  private byte[] delimiters;
  private int currentDelimiterIdx;

  public JsonBlockSerializer(byte[] delimiters) {
    this.objectMapper = new ObjectMapper();
    this.fieldMapping = new FieldMapping();
    this.delimiters = delimiters;
    this.currentDelimiterIdx = 0;
  }

  @Override public SerializedData serialize(Iterator<String> data)
    throws SerializationException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TIntArrayList offsets = new TIntArrayList();
    int currentOffset = 0;
    while (data.hasNext()) {
      String json = data.next();
      try {
        offsets.add(currentOffset);
        byte[] serializedJson = flattenToMap(json);
        out.write(serializedJson);
        currentOffset += serializedJson.length;
      } catch (IOException e) {
        throw new SerializationException(e.getMessage());
      }
    }
    return new SerializedData(out.toByteArray(), offsets.toArray(), fieldMapping);
  }

  private byte[] flattenToMap(String json) throws SerializationException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      flattenJsonTree("", objectMapper.readTree(json), out);
    } catch (IOException e) {
      throw new SerializationException(e.getMessage());
    }
    return out.toByteArray();
  }

  private void flattenJsonTree(String currentPath, JsonNode jsonNode, ByteArrayOutputStream out)
    throws SerializationException {
    if (jsonNode.isObject()) {
      ObjectNode objectNode = (ObjectNode) jsonNode;
      Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
      String pathPrefix = currentPath.isEmpty() ? "" : currentPath + ".";
      while (iter.hasNext()) {
        Map.Entry<String, JsonNode> entry = iter.next();
        flattenJsonTree(pathPrefix + entry.getKey(), entry.getValue(), out);
      }
    } else if (jsonNode.isArray()) {
      throw new SerializationException("Arrays in JSON are not supported yet.");
    } else if (jsonNode.isValueNode()) {
      ValueNode valueNode = (ValueNode) jsonNode;
      if (!fieldMapping.containsField(currentPath)) {
        fieldMapping.put(currentPath, delimiters[currentDelimiterIdx++], getNodeType(jsonNode));
      }
      try {
        byte fieldByte = fieldMapping.getDelimiter(currentPath);
        out.write(fieldByte);
        out.write(valueNode.asText().getBytes());
        out.write(fieldByte);
      } catch (IOException e) {
        throw new SerializationException(e.getMessage());
      }
      // map.put(currentPath, valueNode.asText());
    }
  }

  private DataType getNodeType(JsonNode node) {
    if (node.isTextual()) {
      return DataType.STRING;
    } else if (node.isBoolean()) {
      return DataType.BOOLEAN;
    } else if (node.isInt()) {
      return DataType.INT;
    } else if (node.isLong()) {
      return DataType.LONG;
    } else {
      throw new UnsupportedOperationException("JSON DataType not supported.");
    }
  }
}
