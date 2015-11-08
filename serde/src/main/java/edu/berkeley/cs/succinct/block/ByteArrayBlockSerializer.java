package edu.berkeley.cs.succinct.block;

import edu.berkeley.cs.succinct.SerializationException;
import gnu.trove.list.array.TIntArrayList;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * Serializes a block of byte arrays into a single byte array, separating two adjacent byte arrays
 * by a delimiter. The delimiter is provided as an input, and must not be a part of the input.
 */
public class ByteArrayBlockSerializer implements BlockSerializer<byte[]> {

  private byte recordSeparator;

  /**
   * Constructor to initialize the ByteArrayBlockSerializer.
   *
   * @param recordSeparator The record separator byte to separate records.
   */
  public ByteArrayBlockSerializer(byte recordSeparator) {
    this.recordSeparator = recordSeparator;
  }

  @Override public SerializedData serialize(Iterator<byte[]> data) throws SerializationException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TIntArrayList offsets = new TIntArrayList();
    out.write(recordSeparator);
    int currentOffset = 1;
    while(data.hasNext()) {
      byte[] recordData = data.next();
      try {
        offsets.add(currentOffset);
        out.write(recordData);
        out.write(recordSeparator);
        currentOffset += recordData.length + 1;
      } catch (IOException e) {
        throw new SerializationException(e.getCause());
      }
    }
    return new SerializedData(out.toByteArray(), offsets.toArray(), null);
  }

  /**
   * Set the record separator byte.
   *
   * @param recordSeparator The record separator byte.
   */
  public void setRecordSeparator(byte recordSeparator) {
    this.recordSeparator = recordSeparator;
  }

  /**
   * Get the record separator byte.
   *
   * @return The record separator byte.
   */
  public byte getRecordSeparator() {
    return recordSeparator;
  }
}
