package edu.berkeley.cs.succinct.object.serializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

public class AnnotationSerializer implements ObjectSerializer<String> {

  /**
   * Serialize the data type to an array of bytes. The array of bytes must be safe to use with
   * Succinct interfaces.
   *
   * @param data Input data.
   * @return Serialized array of bytes.
   */
  @Override public byte[] serialize(String data) {
    try (BufferedReader reader = new BufferedReader(new StringReader(data))) {
      String line = reader.readLine();
      while (line != null) {
        String[] annotEntry = line.split("\\^");

        line = reader.readLine();
      }
    } catch (IOException e) {
      System.err.println(e.getMessage());
    }
    return null;
  }
}
