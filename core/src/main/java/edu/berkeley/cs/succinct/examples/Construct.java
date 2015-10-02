package edu.berkeley.cs.succinct.examples;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class Construct {
    public static void main(String[] args) throws IOException {
        if(args.length != 1) {
            System.err.println("Parameters: [input-path] [output-path]");
            System.exit(-1);
        }

        SuccinctFileBuffer succinctFileBuffer;

        if(args[0].endsWith(".succinct")) {
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

        succinctFileBuffer.writeToFile(args[1]);
    }
}
