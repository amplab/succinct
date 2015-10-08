package edu.berkeley.cs.succinct.examples;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.regex.RegexMatch;
import edu.berkeley.cs.succinct.regex.executor.RegExExecutor;
import edu.berkeley.cs.succinct.regex.parser.*;
import edu.berkeley.cs.succinct.regex.planner.NaiveRegExPlanner;
import edu.berkeley.cs.succinct.regex.planner.RegExPlanner;

import java.io.*;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class SuccinctShell {
  private static void printRegex(RegEx re) {
    switch (re.getRegExType()) {
      case Blank: {
        System.out.print("Blank");
        break;
      }
      case Wildcard:
        RegExWildcard w = (RegExWildcard)re;
        System.out.print("Wildcard(");
        printRegex(w.getLeft());
        System.out.print(",");
        printRegex(w.getRight());
        System.out.print(")");
        break;
      case CharRange:
        RegExCharRange c = (RegExCharRange)re;
        System.out.print("CharRange(");
        printRegex(c.getLeft());
        System.out.print(",");
        printRegex(c.getRight());
        System.out.print("," + c.getCharRange() + "," + c.isRepeat() + ")");
        break;
      case Primitive: {
        RegExPrimitive p = ((RegExPrimitive) re);
        System.out.print("Primitive:" + p.getMgram());
        break;
      }
      case Repeat: {
        RegExRepeat r = (RegExRepeat)re;
        System.out.print("Repeat(");
        printRegex(r.getInternal());
        System.out.print(")");
        break;
      }
      case Concat: {
        RegExConcat co = (RegExConcat)re;
        System.out.print("Concat(");
        printRegex(co.getLeft());
        System.out.print(",");
        printRegex(co.getRight());
        System.out.print(")");
        break;
      }
      case Union: {
        RegExUnion u = (RegExUnion)re;
        System.out.print("Union(");
        printRegex(u.getFirst());
        System.out.print(",");
        printRegex(u.getSecond());
        System.out.print(")");
        break;
      }
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Parameters: [input-path]");
      System.exit(-1);
    }

    SuccinctFileBuffer succinctFileBuffer;

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

    BufferedReader shellReader = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      System.out.print("succinct> ");
      String command = shellReader.readLine();
      String[] cmdArray = command.split(" ");
      if (cmdArray[0].compareTo("count") == 0) {
        if (cmdArray.length != 2) {
          System.err.println("Could not parse count query.");
          System.err.println("Usage: count [query]");
          continue;
        }
        System.out.println(
          "Count[" + cmdArray[1] + "] = " + succinctFileBuffer.count(cmdArray[1].getBytes()));
      } else if (cmdArray[0].compareTo("search") == 0) {
        if (cmdArray.length != 2) {
          System.err.println("Could not parse search query.");
          System.err.println("Usage: search [query]");
          continue;
        }
        Long[] results = succinctFileBuffer.search(cmdArray[1].getBytes());
        System.out.println("Result size = " + results.length);
        System.out.print("Search[" + cmdArray[1] + "] = {");
        if (results.length < 10) {
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
      } else if (cmdArray[0].compareTo("extract") == 0) {
        if (cmdArray.length != 3) {
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
        System.out.println("Extract[" + offset + ", " + length + "] = " + new String(
          succinctFileBuffer.extract(offset, length)));
      } else if (cmdArray[0].compareTo("regex") == 0) {
        if (cmdArray.length != 2) {
          System.err.println("Could not parse regex query.");
          System.err.println("Usage: regex [query]");
          continue;
        }

        Map<Long, Integer> results;
        try {
          RegExParser parser = new RegExParser(new String(cmdArray[1]));
          RegEx regEx = parser.parse();

          System.out.println("Parsed Expression: ");
          printRegex(regEx);
          System.out.println();

          RegExPlanner planner = new NaiveRegExPlanner(succinctFileBuffer, regEx);
          RegEx optRegEx = planner.plan();

          RegExExecutor regExExecutor = new RegExExecutor(succinctFileBuffer, optRegEx);
          regExExecutor.execute();

          Set<RegexMatch> chunkResults = regExExecutor.getFinalResults();
          results = new TreeMap<Long, Integer>();
          for (RegexMatch result : chunkResults) {
            results.put(result.getOffset(), result.getLength());
          }
        } catch (RegExParsingException e) {
          System.err.println("Could not parse regular expression: [" + cmdArray[1] + "]");
          continue;
        }
        System.out.println("Result size = " + results.size());
        System.out.print("Regex[" + cmdArray[1] + "] = {");
        int count = 0;
        for (Map.Entry<Long, Integer> entry : results.entrySet()) {
          if (count >= 10)
            break;
          System.out.print("offset = " + entry.getKey() + "; len = " + entry.getValue() + ", ");
          count++;
        }
        System.out.println("...}");
      } else if (cmdArray[0].compareTo("quit") == 0) {
        System.out.println("Quitting...");
        break;
      } else {
        System.err
          .println("Unknown command. Command must be one of: count, search, extract, quit.");
        continue;
      }
    }
  }
}
