package edu.berkeley.cs.succinct.block.json;

import edu.berkeley.cs.succinct.DataType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class FieldMapping implements Serializable {
  private Map<Byte, String> delimiter2fieldMap;
  private Map<String, Byte> field2DelimiterMap;
  private Map<String, DataType> field2TypeMap;

  public FieldMapping() {
    this.delimiter2fieldMap = new HashMap<>();
    this.field2DelimiterMap = new HashMap<>();
    this.field2TypeMap = new HashMap<>();
  }

  public boolean containsField(String field) {
    return field2DelimiterMap.containsKey(field);
  }

  public boolean containsDelimiter(byte delimiter) {
    return delimiter2fieldMap.containsKey(delimiter);
  }

  public void put(String field, byte delimiter, DataType dataType) {
    field2DelimiterMap.put(field, delimiter);
    field2TypeMap.put(field, dataType);
    delimiter2fieldMap.put(delimiter, field);
  }

  public void updateType(String field, DataType dataType) {
    field2TypeMap.put(field, dataType);
  }

  public byte getDelimiter(String field) {
    return field2DelimiterMap.get(field);
  }

  public DataType getDataType(String field) {
    return field2TypeMap.get(field);
  }

  public String getField(byte delimiter) {
    return delimiter2fieldMap.get(delimiter);
  }

  public int size() {
    return field2DelimiterMap.size();
  }
}
