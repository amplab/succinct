package edu.berkeley.cs.succinct.examples;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;

import java.io.*;
import java.util.Map;

public class SuccinctShell {
    public static void main(String[] args) throws IOException {
        if(args.length != 1) {
            System.err.println("Paramters: [input-path]");
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

        BufferedReader shellReader = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            System.out.print("succinct> ");
            String command = shellReader.readLine();
            String[] cmdArray = command.split(" ");
            if(cmdArray[0].compareTo("count") == 0) {
                if(cmdArray.length != 2) {
                    System.err.println("Could not parse count query.");
                    System.err.println("Usage: count [query]");
                    continue;
                }
                System.out.println("Count[" + cmdArray[1] + "] = " + succinctFileBuffer.count(cmdArray[1].getBytes()));
            } else if(cmdArray[0].compareTo("search") == 0) {
                if(cmdArray.length != 2) {
                    System.err.println("Could not parse search query.");
                    System.err.println("Usage: search [query]");
                    continue;
                }
                Long[] results = succinctFileBuffer.search(cmdArray[1].getBytes());
                System.out.println("Result size = " + results.length);
                System.out.print("Search[" + cmdArray[1] + "] = {");
                if(results.length < 10) {
                    for (int i = 0; i < results.length; i++) {
                        System.out.print(results[i] + ", ");
                    }
                    System.out.println("}");
                } else {
                    for (int i = 0; i < 10; i++) {
                        System.out.print(results[i] + ", ");
                    }
                    System.out.println("...}");
                }
            } else if(cmdArray[0].compareTo("extract") == 0) {
                if(cmdArray.length != 3) {
                    System.err.println("Could not parse extract query.");
                    System.err.println("Usage: extract [offset] [length]");
                    continue;
                }
                Integer offset, length;
                try {
                    offset = Integer.parseInt(cmdArray[1]);
                } catch (Exception e) {
                    System.err.println("[Extract]: Failed to parse offset: must be an integer.");
                    continue;
                }
                try {
                    length = Integer.parseInt(cmdArray[2]);
                } catch (Exception e) {
                    System.err.println("[Extract]: Failed to parse length: must be an integer.");
                    continue;
                }
                System.out.println("Extract[" + offset + ", " + length + "] = " + new String(succinctFileBuffer.extract(offset, length)));
            } else if (cmdArray[0].compareTo("regex") == 0) {
                if(cmdArray.length != 2) {
                    System.err.println("Could not parse regex query.");
                    System.err.println("Usage: regex [query]");
                    continue;
                }

                Map<Long, Integer> results = null;
                try {
                    results = succinctFileBuffer.regexSearch(cmdArray[1]);
                } catch (RegExParsingException e) {
                    System.err.println("Could not parse regular expression: [" + cmdArray[1] + "]");
                    continue;
                }
                System.out.println("Result size = " + results.size());
                System.out.print("Regex[" + cmdArray[1] + "] = {");
                int count = 0;
                for (Map.Entry<Long, Integer> entry: results.entrySet()) {
                    if (count >= 10) break;
                    System.out.print("offset = " + entry.getKey() + "; len = " + entry.getValue() + ", ");
                    count++;
                }
                System.out.println("...}");
            } else if(cmdArray[0].compareTo("quit") == 0) {
                System.out.println("Quitting...");
                break;
            } else {
                System.err.println("Unknown command. Command must be one of: count, search, extract, quit.");
                continue;
            }
        }
    }
}
