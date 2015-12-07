package edu.berkeley.cs.succinct.util;

import java.io.*;

public class IOUtils {

  /**
   * Reads an integer array from stream.
   *
   * @param is DataInputStream to read data from.
   * @return Array read from stream.
   */
  public static int[] readArray(DataInputStream is) {
    int[] A = null;

    try {
      int length = is.readInt();
      A = new int[length];
      for (int i = 0; i < length; i++) {
        A[i] = is.readInt();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return A;
  }

  /**
   * Writes an integer array to stream.
   *
   * @param array Array to write to stream.
   * @param os DataOutputStream to write to.
   * @throws IOException
   */
  public static void writeArray(int[] array, DataOutputStream os) throws IOException {
    os.writeInt(array.length);
    for (int i = 0; i < array.length; i++) {
      os.writeInt(array[i]);
    }
  }

  /**
   * Opens an output stream for the specified path.
   *
   * @param path File path where output stream is to be opened.
   * @return The output stream.
   * @throws FileNotFoundException
   */
  public static DataOutputStream getOutputStream(String path) throws FileNotFoundException {
    return new DataOutputStream(new FileOutputStream(path));
  }

  /**
   * Opens an input stream for the specified path.
   *
   * @param path File path where input stream is to be opened.
   * @return The input stream.
   * @throws FileNotFoundException
   */
  public static DataInputStream getInputStream(String path) throws FileNotFoundException {
    return new DataInputStream(new FileInputStream(path));
  }

  /**
   * Checks for invalid bytes in the input.
   *
   * @param input Array of bytes.
   * @return First offset where an invalid byte occurs; -1 if all bytes are valid.
   */
  public static int checkBytes(byte[] input) {
    for (int i = 0; i < input.length; i++) {
      if (input[i] < 0) {
        return i;
      }
    }
    return -1;
  }
}
