package edu.berkeley.cs.succinct;

public class PrimitiveDeserializer {

  public String deserializeString(byte[] data) {
    return new String(data);
  }

  public boolean deserialzeBoolean(byte[] data) throws DeserializationException {
    if (data.length != 1) {
      throw new DeserializationException("Invalid number of bytes for boolean.");
    }

    if (data[0] != PrimitiveSerializer.SERIALIZED_TRUE && data[0] != PrimitiveSerializer.SERIALIZED_FALSE) {
      throw new DeserializationException("Invalid boolean data.");
    }

    return data[0] == PrimitiveSerializer.SERIALIZED_TRUE;
  }

  public byte deserializeByte(byte[] data) throws DeserializationException {
    return Byte.parseByte(new String(data));
  }

  public short deserializeShort(byte[] data) throws DeserializationException {
    return Short.parseShort(new String(data));
  }

  public int deserializeInt(byte[] data) throws DeserializationException {
    return Integer.parseInt(new String(data));
  }

  public long deserializeLong(byte[] data) throws DeserializationException {
    return Long.parseLong(new String(data));
  }
}
