package edu.berkeley.cs.succinct.util.container;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public final class IntArray implements BasicArray {
  private IntBuffer m_A;
  private int m_pos;
  private static final int INT_SIZE_IN_BYTES = 4;

  public IntArray(IntArray A, int pos) {
    m_A = A.m_A;
    m_pos = pos;
  }

  public IntArray(int size) {
    m_A = ByteBuffer.allocateDirect(size * INT_SIZE_IN_BYTES).asIntBuffer();
    m_pos = 0;
  }

  public int get(int i) {
    return m_A.get(m_pos + i);
  }

  public void set(int i, int val) {
    m_A.put(m_pos + i, val);
  }

  public int update(int i, int val) {
    int tmp = m_A.get(m_pos + i) + val;
    m_A.put(m_pos + i, tmp);
    return tmp;
  }

  public int length() {
    return m_A.capacity() - m_pos;
  }

  public void destroy() {
    m_A = null;
  }
}
