package edu.berkeley.cs.succinct.util.dictionary;

import edu.berkeley.cs.succinct.util.CommonUtils;

import java.util.ArrayList;
import java.util.HashMap;

public class Tables {
  public static char[] offsetBits = new char[17];
  public static char[][] smallRank = new char[65536][16];
  public static int[][] decodeTable = new int[17][];
  public static ArrayList<HashMap<Integer, Integer>> encodeTable = new ArrayList<>();

  static {
    short[] C16 = new short[17];
    C16[0] = 2;
    offsetBits[0] = 1;
    C16[1] = 16;
    offsetBits[1] = 4;
    C16[2] = 120;
    offsetBits[2] = 7;
    C16[3] = 560;
    offsetBits[3] = 10;
    C16[4] = 1820;
    offsetBits[4] = 11;
    C16[5] = 4368;
    offsetBits[5] = 13;
    C16[6] = 8008;
    offsetBits[6] = 13;
    C16[7] = 11440;
    offsetBits[7] = 14;
    C16[8] = 12870;
    offsetBits[8] = 14;

    int[] q = new int[17];
    for (int i = 0; i <= 16; i++) {
      if (i > 8) {
        C16[i] = C16[16 - i];
        offsetBits[i] = offsetBits[16 - i];
      }
      decodeTable[i] = new int[C16[i]];
      encodeTable.add(new HashMap<Integer, Integer>());
      q[i] = 0;
    }
    q[16] = 1;
    for (int i = 0; i <= 65535; i++) {
      int p = CommonUtils.popCount(i);
      decodeTable[p % 16][q[p]] = i;
      encodeTable.get(p % 16).put(i, q[p]);
      q[p]++;
      for (int j = 0; j < 16; j++) {
        smallRank[i][j] = (char) CommonUtils.popCount(i >> (15 - j));
      }
    }
  }
}
