package edu.berkeley.cs.succinct.block;

import edu.berkeley.cs.succinct.SerializationException;

import java.util.Iterator;

/**
 * Serialization/Deserialization interface for a block of data of arbitrary objects. The serialized
 * array of bytes must be safe to use with Succinct interfaces. The block is an iterator over
 * objects. This is useful for data types that share information across object instances, such
 * that their serialization/deserialization is only meaningful in blocks. Examples include
 * JSON objects (which may share fields across object instances), table rows (which share a fixed
 * schema across object instances) and dictionary encoded strings (which share dictionary terms
 * across object instances).
 *
 *
 * @param <T> Type of the data in the block.
 */
public interface BlockSerializer<T> {

  class SerializedData {
    private byte[] data;
    private int[] offsets;
    private Object metadata;

    public SerializedData(byte[] data, int[] offsets, Object metadata) {
      this.data = data;
      this.offsets = offsets;
      this.metadata = metadata;
    }

    public byte[] getData() {
      return data;
    }

    public int[] getOffsets() {
      return offsets;
    }

    public Object getMetadata() {
      return metadata;
    }
  }

  /**
   * Serializes a block of data (represented as an iterator over the generic data type) into an
   * array of bytes. The array of bytes must be safe to use with Succinct interfaces.
   *
   * @param data Block of data to be serialized.
   * @return The serialized array of bytes.
   */
  SerializedData serialize(Iterator<T> data) throws SerializationException;
}
