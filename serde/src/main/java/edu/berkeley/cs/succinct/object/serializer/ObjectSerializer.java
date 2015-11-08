package edu.berkeley.cs.succinct.object.serializer;

/**
 * Serialization interface for arbitrary objects. The serialized array of bytes
 * must be safe to use with Succinct interfaces.
 *
 * @param <T> Object type to serialize.
 */
public interface ObjectSerializer<T> {

  /**
   * Serialize the data type to an array of bytes. The array of bytes must be safe to use with
   * Succinct interfaces.
   *
   * @param data Input data.
   * @return Serialized array of bytes.
   */
  byte[] serialize(T data);
}
