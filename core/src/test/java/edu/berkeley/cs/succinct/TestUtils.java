package edu.berkeley.cs.succinct;

import java.util.HashSet;

public class TestUtils {
  /**
   * Naive string count algorithm. This is the baseline we test against.
   *
   * @param input Input string.
   * @param str   Query string.
   * @return Number of occurrences of the query string in the input string.
   */
  public static int stringCount(String input, String str) {
    int count = 0;
    for (int i = 0; i < input.length(); i++) {
      int j = 0;
      for (; j < str.length(); j++) {
        if (input.charAt(i + j) != str.charAt(j)) {
          break;
        }
      }
      if (j == str.length()) {
        count++;
      }
    }
    return count;
  }

  /**
   * Naive string record count algorithm. This is the baseline we test against.
   *
   * @param input   Input string.
   * @param offsets Offsets marking beginning of new records.
   * @param str     Query string.
   * @return Number of records in the input string which contain the query string.
   */
  public static int stringRecordCount(String input, int[] offsets, String str) {
    HashSet<Integer> records = new HashSet<>();
    for (int i = 0; i < input.length(); i++) {
      int j = 0;
      for (; j < str.length(); j++) {
        if (input.charAt(i + j) != str.charAt(j)) {
          break;
        }
      }
      if (j == str.length()) {
        int k = 0;
        for (; k < offsets.length; k++) {
          if (offsets[k] > i)
            break;
        }
        records.add(k - 1);
      }
    }
    return records.size();
  }
}
