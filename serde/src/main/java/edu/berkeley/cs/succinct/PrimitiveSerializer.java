package edu.berkeley.cs.succinct;

public class PrimitiveSerializer {

  public static final byte SERIALIZED_TRUE = ((byte) '1');
  public static final byte SERIALIZED_FALSE = ((byte) '0');

  /**
   * Serialize a String to an array bytes.
   *
   * @param data String data.
   * @return Serialized Array of bytes.
   */
  static byte[] serialize(String data) throws SerializationException {
    return data.getBytes();
  }

  static byte[] serialize(boolean data) throws SerializationException  {
    return data ? new byte[] {SERIALIZED_TRUE} : new byte[] {SERIALIZED_FALSE};
  }

  static byte[] serialize(byte data) throws SerializationException {
    return String.valueOf(data).getBytes();
  }

  static byte[] serialize(byte data, int width) throws SerializationException {
    return String.format("%0" + width + "d", data).getBytes();
  }

  static byte[] serialize(short data) throws SerializationException {
    return String.valueOf(data).getBytes();
  }

  static byte[] serialize(short data, int width) throws SerializationException {
    return String.format("%0" + width + "d", data).getBytes();
  }

  static byte[] serialize(int data) throws SerializationException {
    return String.valueOf(data).getBytes();
  }

  static byte[] serialize(int data, int width) throws SerializationException {
    return String.format("%0" + width + "d", data).getBytes();
  }

  static byte[] serialize(long data) throws SerializationException {
    return String.valueOf(data).getBytes();
  }

  static byte[] serialize(long data, int width) throws SerializationException {
    return String.format("%0" + width + "d", data).getBytes();
  }
}
