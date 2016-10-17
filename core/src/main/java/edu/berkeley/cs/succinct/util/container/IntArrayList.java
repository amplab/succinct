package edu.berkeley.cs.succinct.util.container;

public class IntArrayList {

  private static final int FBS = 16;
  private static final int FBS_HIBIT = 4;
  private static final int NUM_BUCKETS = 32;
  private int[][] buckets;
  private int currentIdx;

  public IntArrayList() {
    buckets = new int[NUM_BUCKETS][];
    buckets[0] = new int[FBS];
    for (int i = 1; i < NUM_BUCKETS; i++) {
      buckets[i] = null;
    }
    currentIdx = 0;
  }

  public void add(int val) {
    int pos = currentIdx++ + FBS;
    int hibit = 31 - Integer.numberOfLeadingZeros(pos);
    int bucketOff = pos ^ (1 << hibit);
    int bucketIdx = hibit - FBS_HIBIT;
    if (buckets[bucketIdx] == null) {
      int size = (1 << (bucketIdx + FBS_HIBIT));
      buckets[bucketIdx] = new int[size];
    }
    buckets[bucketIdx][bucketOff] = val;
  }

  public int get(int idx) {
    int pos = idx + FBS;
    int hibit = 31 - Integer.numberOfLeadingZeros(pos);
    int bucketOff = pos ^ (1 << hibit);
    int bucketIdx = hibit - FBS_HIBIT;
    return buckets[bucketIdx][bucketOff];
  }

  public int size() {
    return currentIdx;
  }

  public int[] toArray() {
    int[] out = new int[currentIdx];
    int outOffset = 0;
    int i;
    for (i = 0; i < NUM_BUCKETS - 1 && buckets[i + 1] != null; i++) {
      System.arraycopy(buckets[i], 0, out, outOffset, buckets[i].length);
      outOffset += buckets[i].length;
    }
    System.arraycopy(buckets[i], 0, out, outOffset, currentIdx - outOffset);
    return out;
  }
}
