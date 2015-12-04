package edu.berkeley.cs.succinct.util.stream.serops;

import edu.berkeley.cs.succinct.util.stream.LongArrayStream;

import java.io.IOException;

public class IntVectorOps {

  static public int get(LongArrayStream data, int index, int bitWidth) throws IOException {
    return (int) BitVectorOps.getValue(data, index * bitWidth, bitWidth);
  }
}
