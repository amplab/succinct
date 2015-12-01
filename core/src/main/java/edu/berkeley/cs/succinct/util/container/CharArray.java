package edu.berkeley.cs.succinct.util.container;

public final class CharArray implements BasicArray {
  private char[] m_A;
  private int m_pos;

  public CharArray(char[] A) {
    m_A = A;
    m_pos = 0;
  }

  public int get(int i) {
    return m_A[m_pos + i] & 0xffff;
  }

  public void set(int i, int val) {
    m_A[m_pos + i] = (char) (val & 0xffff);
  }

  public int update(int i, int val) {
    return m_A[m_pos + i] += val & 0xffff;
  }

  public int length() {
    return m_A.length;
  }

  public void destroy() {
    m_A = null;
  }
}
