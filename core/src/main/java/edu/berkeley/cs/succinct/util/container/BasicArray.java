package edu.berkeley.cs.succinct.util.container;

public interface BasicArray {
  int get(int i);

  void set(int i, int val);

  int update(int i, int val);

  int length();

  void destroy();
}
