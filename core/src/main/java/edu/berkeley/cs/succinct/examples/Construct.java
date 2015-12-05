package edu.berkeley.cs.succinct.examples;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctCore;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;

public class Construct {
  public static void main(String[] args) throws IOException {
    if (args.length < 2 || args.length > 3) {
      System.err.println("Parameters: [input-path] [output-path] <[type]>");
      System.exit(-1);
    }

    SuccinctCore.logger.setLevel(Level.ALL);
    SuccinctFileBuffer succinctFileBuffer;

    String type = "file";
    if (args.length == 3) {
      type = args[2];
    }

    long start = System.currentTimeMillis();

    switch (type) {
      case "file":
        if (args[0].endsWith(".succinct")) {
          succinctFileBuffer = new SuccinctFileBuffer(args[0], StorageMode.MEMORY_ONLY);
        } else {
          File file = new File(args[0]);
          if (file.length() > 1L << 31) {
            System.err.println("Cant handle files > 2GB");
            System.exit(-1);
          }
          byte[] fileData = new byte[(int) file.length()];
          System.out.println("File size: " + fileData.length + " bytes");
          DataInputStream dis = new DataInputStream(new FileInputStream(file));
          dis.readFully(fileData, 0, (int) file.length());

          succinctFileBuffer = new SuccinctFileBuffer(fileData);
        }
        break;
      case "indexed-file":
        if (args[0].endsWith(".succinct")) {
          succinctFileBuffer = new SuccinctIndexedFileBuffer(args[0], StorageMode.MEMORY_ONLY);
        } else {
          File file = new File(args[0]);
          if (file.length() > 1L << 31) {
            System.err.println("Cant handle files > 2GB");
            System.exit(-1);
          }
          byte[] fileData = new byte[(int) file.length()];
          System.out.println("File size: " + fileData.length + " bytes");
          DataInputStream dis = new DataInputStream(new FileInputStream(file));
          dis.readFully(fileData, 0, (int) file.length());

          ArrayList<Integer> positions = new ArrayList<>();
          positions.add(0);
          for (int i = 0; i < fileData.length; i++) {
            if (fileData[i] == '\n') {
              positions.add(i + 1);
            }
          }
          int[] offsets = new int[positions.size()];
          for (int i = 0; i < offsets.length; i++) {
            offsets[i] = positions.get(i);
          }
          succinctFileBuffer = new SuccinctIndexedFileBuffer(fileData, offsets);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported mode: " + type);
    }

    long end = System.currentTimeMillis();
    System.out.println("Time to construct: " + (end - start) / 1000 + "s");

    succinctFileBuffer.writeToFile(args[1]);
  }
}
