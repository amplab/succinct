package edu.berkeley.cs.succinct.object.deserializer;

import edu.berkeley.cs.succinct.DeserializationException;

/**
 * Deserialization interface for arbitrary objects. The serialized array of bytes must conform to
 * the serializer interface.
 *
 * @param <T> Object type to deserialize.
 */
public interface ObjectDeserializer<T> {

  /**
   * Deserialize the array of bytes to the original data type.
   *
   * @param data Input array of bytes.
   * @return Deserialized data.
   */
  T deserialize(byte[] data) throws DeserializationException;
}
