package edu.berkeley.cs.succinct.examples;

import edu.berkeley.cs.succinct.SuccinctCore;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
import edu.berkeley.cs.succinct.util.container.IntArrayList;

import java.io.*;
import java.util.logging.Level;

public class Construct {
  public static void main(String[] args) throws IOException {
    if (args.length < 2 || args.length > 3) {
      System.err.println("Parameters: [input-path] [output-path] <[type]>");
      System.exit(-1);
    }

    File file = new File(args[0]);
    if (file.length() > 1L << 31) {
      System.err.println("Cant handle files > 2GB");
      System.exit(-1);
    }

    byte[] fileData = new byte[(int) file.length()];
    System.out.println("File size: " + fileData.length + " bytes");
    DataInputStream dis = new DataInputStream(new FileInputStream(file));
    dis.readFully(fileData, 0, (int) file.length());

    FileOutputStream fos = new FileOutputStream(args[1]);
    DataOutputStream os = new DataOutputStream(fos);

    String type = "file";
    if (args.length == 3) {
      type = args[2];
    }

    long start = System.currentTimeMillis();

    SuccinctCore.LOG.setLevel(Level.ALL);
    switch (type) {
      case "file": {
        SuccinctFileBuffer.construct(fileData, os);
        break;
      }
      case "indexed-file": {
        IntArrayList offsets = new IntArrayList();
        offsets.add(0);
        for (int i = 0; i < fileData.length; i++) {
          if (fileData[i] == '\n') {
            offsets.add(i + 1);
          }
        }
        SuccinctIndexedFileBuffer.construct(fileData, offsets.toArray(), os);
        break;
      }
      default:
        throw new UnsupportedOperationException("Unsupported mode: " + type);
    }

    long end = System.currentTimeMillis();
    System.out.println("Time to construct: " + (end - start) / 1000 + "s");

  }
}
