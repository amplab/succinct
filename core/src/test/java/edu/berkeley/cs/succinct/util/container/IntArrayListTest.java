package edu.berkeley.cs.succinct.util.container;

import junit.framework.TestCase;

public class IntArrayListTest extends TestCase {

  public void testList() throws Exception {
    IntArrayList list = new IntArrayList();
    for (int i = 0; i < 100000; i++) {
      list.add(i);
    }

    assertEquals(100000, list.size());

    for (int i = 0; i < 100000; i++) {
      assertEquals(i, list.get(i));
    }
  }

  public void testToArray() throws Exception {
    IntArrayList list = new IntArrayList();
    for (int i = 0; i < 100000; i++) {
      list.add(i);
    }

    int[] arr = list.toArray();

    assertEquals(100000, arr.length);

    for (int i = 0; i < 100000; i++) {
      assertEquals(i, arr[i]);
    }
  }
}
