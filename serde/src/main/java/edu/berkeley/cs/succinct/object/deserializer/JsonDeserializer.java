package edu.berkeley.cs.succinct.object.deserializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.berkeley.cs.succinct.DataType;
import edu.berkeley.cs.succinct.DeserializationException;
import edu.berkeley.cs.succinct.PrimitiveDeserializer;
import edu.berkeley.cs.succinct.block.json.FieldMapping;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class JsonDeserializer implements ObjectDeserializer<String> {

  FieldMapping fieldMapping;
  ObjectMapper mapper;

  public JsonDeserializer(FieldMapping fieldMapping) {
    this.fieldMapping = fieldMapping;
    this.mapper = new ObjectMapper();
  }

  @Override public String deserialize(byte[] data) throws DeserializationException {
    return mapToJsonString(deserializeToMap(data));
  }

  public String deserialize(long id, byte[] data) throws DeserializationException {
    Map<String, Object> jsonMap = deserializeToMap(data);
    jsonMap.put("id", id);
    return mapToJsonString(jsonMap);
  }

  private String mapToJsonString(Map<String, Object> jsonMap) throws DeserializationException {
    String jsonString;
    try {
      jsonString = mapper.writeValueAsString(jsonMap);
    } catch (JsonProcessingException e) {
      throw new DeserializationException(e.getMessage());
    }

    return jsonString;
  }

  private Map<String, Object> deserializeToMap(byte[] data) throws DeserializationException {
    Map<String, Object> jsonMap = new HashMap<String, Object>();

    int curOffset = 0;
    while (curOffset < data.length) {
      byte curDelimiter = data[curOffset++];
      String fieldKey = fieldMapping.getField(curDelimiter);
      DataType fieldType = fieldMapping.getDataType(fieldKey);
      byte[] fieldValueBytes = extractField(data, curOffset, curDelimiter);
      Object fieldValue = PrimitiveDeserializer.deserializePrimitive(fieldValueBytes, fieldType);
      add(jsonMap, fieldKey, fieldValue);
      curOffset += (fieldValueBytes.length + 1); // Skip the field data, and the end delimiter
    }

    return jsonMap;
  }

  private void add(Map<String, Object> map, String key, Object value) {
    if (key.contains(".")) {
      String[] keySplits = key.split("\\.", 2);
      if (map.containsKey(keySplits[0])) {
        @SuppressWarnings("unchecked")
        Map<String, Object> valObj = (Map<String, Object>) map.get(keySplits[0]);
        add(valObj, keySplits[1], value);
      } else {
        Map<String, Object> valObj = new HashMap<String, Object>();
        add(valObj, keySplits[1], value);
        map.put(keySplits[0], valObj);
      }
    } else {
      map.put(key, value);
    }
  }

  private byte[] extractField(byte[] data, int startOffset, byte delimiter) {
    int i = startOffset;
    while (data[i] != delimiter) {
      i++;
    }
    return Arrays.copyOfRange(data, startOffset, i);
  }

  private Object readField(byte[] data, int startOffset, byte delimiter, DataType dataType) {
    int i = startOffset;
    String field = "";
    while (data[i] != delimiter) {
      field += (char) data[i++];
    }
    return field;
  }


}
