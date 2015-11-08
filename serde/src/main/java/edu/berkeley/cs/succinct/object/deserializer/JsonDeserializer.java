package edu.berkeley.cs.succinct.object.deserializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.berkeley.cs.succinct.DataType;
import edu.berkeley.cs.succinct.DeserializationException;
import edu.berkeley.cs.succinct.block.json.FieldMapping;

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
    if (data.length == 0) {
      return "{}";
    }

    Map<String, Object> jsonMap = new HashMap<String, Object>();
    int curOffset = 0;
    while (curOffset < data.length) {
      byte curDelimiter = data[curOffset++];
      String fieldKey = fieldMapping.getField(curDelimiter);
      DataType fieldType = fieldMapping.getDataType(fieldKey);
      Object fieldValue = readField(data, curOffset, curDelimiter, fieldType );
      add(jsonMap, fieldKey, fieldValue);
      curOffset += (fieldValue.length() + 1); // Skip the field data, and the end delimiter
    }

    String jsonString;
    try {
      jsonString = mapper.writeValueAsString(jsonMap);
    } catch (JsonProcessingException e) {
      throw new DeserializationException(e.getMessage());
    }

    return jsonString;
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

  private Object readField(byte[] data, int startOffset, byte delimiter, DataType dataType) {
    int i = startOffset;
    String field = "";
    while (data[i] != delimiter) {
      field += (char) data[i++];
    }
    return field;
  }


}
