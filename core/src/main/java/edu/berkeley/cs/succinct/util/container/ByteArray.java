package edu.berkeley.cs.succinct.util.container;

public final class ByteArray implements BasicArray {
  private byte[] m_A;
  private int m_pos;

  public ByteArray(byte[] A) {
    m_A = A;
    m_pos = 0;
  }

  public int get(int i) {
    return m_A[m_pos + i] & 0xff;
  }

  public void set(int i, int val) {
    m_A[m_pos + i] = (byte) (val & 0xff);
  }

  public int update(int i, int val) {
    return m_A[m_pos + i] += val & 0xff;
  }

  public int length() {
    return m_A.length;
  }

  public void destroy() {
    m_A = null;
  }
}
