package edu.berkeley.cs.succinct;

public class PrimitiveDeserializer {

  public static String deserializeString(byte[] data) {
    return new String(data);
  }

  public static Boolean deserialzeBoolean(byte[] data) throws DeserializationException {
    if (data.length != 1) {
      throw new DeserializationException("Invalid number of bytes for boolean.");
    }

    if (data[0] != PrimitiveSerializer.SERIALIZED_TRUE && data[0] != PrimitiveSerializer.SERIALIZED_FALSE) {
      throw new DeserializationException("Invalid boolean data.");
    }

    return data[0] == PrimitiveSerializer.SERIALIZED_TRUE;
  }

  public static Byte deserializeByte(byte[] data) throws DeserializationException {
    return Byte.parseByte(new String(data));
  }

  public static Short deserializeShort(byte[] data) throws DeserializationException {
    return Short.parseShort(new String(data));
  }

  public static Integer deserializeInt(byte[] data) throws DeserializationException {
    return Integer.parseInt(new String(data));
  }

  public static Long deserializeLong(byte[] data) throws DeserializationException {
    return Long.parseLong(new String(data));
  }

  public static Object deserializePrimitive(byte[] field, DataType dataType)
    throws DeserializationException {
    switch (dataType) {
      case STRING: return PrimitiveDeserializer.deserializeString(field);
      case BOOLEAN: return PrimitiveDeserializer.deserialzeBoolean(field);
      case BYTE: return PrimitiveDeserializer.deserializeByte(field);
      case SHORT: return PrimitiveDeserializer.deserializeShort(field);
      case INT: return PrimitiveDeserializer.deserializeInt(field);
      case LONG: return PrimitiveDeserializer.deserializeLong(field);
      default: throw new DeserializationException("Deserialize using custom deserializer.");
    }
  }
}
