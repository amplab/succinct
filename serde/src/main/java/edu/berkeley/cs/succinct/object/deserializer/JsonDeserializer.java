package edu.berkeley.cs.succinct.object.deserializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.berkeley.cs.succinct.DataType;
import edu.berkeley.cs.succinct.DeserializationException;
import edu.berkeley.cs.succinct.PrimitiveDeserializer;
import edu.berkeley.cs.succinct.block.json.FieldMapping;

import java.util.*;

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

  private Map<String, Object> deserializeToMap(byte[] data) throws DeserializationException, IllegalArgumentException {
    Map<String, Object> jsonMap = new HashMap<String, Object>();

    int curOffset = 0;
    while (curOffset < data.length) {
      byte curDelimiter = data[curOffset++];
      String fieldKey = fieldMapping.getField(curDelimiter);
      DataType fieldType = fieldMapping.getDataType(fieldKey);
      Object fieldValue;
      int totalLength;
      if (isArray(fieldType)) {
        ArrayList<byte []> fieldValuesBytes = extractArrayField(data, curOffset, curDelimiter);
        final DataType typeinArray = getPrimitiveFromArray(fieldType);
        ArrayList<Object> resultSet = new ArrayList<>();
        totalLength = 0;
        for (byte [] fieldValueBytes : fieldValuesBytes) {
          resultSet.add(PrimitiveDeserializer.deserializePrimitive(fieldValueBytes, typeinArray));
          totalLength += fieldValueBytes.length + 2; // Skip the field data, and the delimiter at end and start of next field
        }
        fieldValue = resultSet.toArray();
        totalLength -= 1; //  Remove last additional append of delimiter
      }
      else {
        byte[] fieldValueBytes = extractField(data, curOffset, curDelimiter);
        fieldValue = PrimitiveDeserializer.deserializePrimitive(fieldValueBytes, fieldType);
        totalLength = fieldValueBytes.length + 1; // Skip the field data, and the end delimiter
      }
      add(jsonMap, fieldKey, fieldValue);
      curOffset += totalLength;
    }

    return jsonMap;
  }

  private DataType getPrimitiveFromArray(DataType fieldType) throws IllegalArgumentException {
    if (fieldType == DataType.STRINGARRAY) return DataType.STRING;
    else if (fieldType == DataType.LONGARRAY) return DataType.LONG;
    else if (fieldType == DataType.BYTEARRAY) return DataType.BYTE;
    else if (fieldType == DataType.BOOLARRAY) return DataType.BOOLEAN;
    else throw new IllegalArgumentException("Called getPrimitiveFromArray on a non Array DataType");
  }

  private Boolean isArray(DataType fieldType) {
    return fieldType == DataType.STRINGARRAY || fieldType == DataType.LONGARRAY ||
            fieldType == DataType.BYTEARRAY || fieldType == DataType.BOOLARRAY;
  }

  private void add(Map<String, Object> map, String key, Object value) {
    if (key.contains(".")) {
      String[] keySplits = key.split("\\.", 2);
      if (map.containsKey(keySplits[0])) {
        @SuppressWarnings("unchecked") Map<String, Object> valObj =
          (Map<String, Object>) map.get(keySplits[0]);
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

  private ArrayList<byte []> extractArrayField(byte[] data, int startOffset, byte delimiter) {
    int i = startOffset;
    ArrayList<byte []> res = new ArrayList<>();
    while (true) {
      byte[] result = extractField(data, i, delimiter);
      res.add(result);
      i += result.length + 1;
      if(data.length <= i || data[i] != delimiter) break;
      else i++;
    }
    return res;
  }

}
