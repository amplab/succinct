package edu.berkeley.cs.succinct.util;

public class Pair<T1, T2> {

  public T1 first;
  public T2 second;

  /**
   * Constructor to initialize pair
   *
   * @param first  First element.
   * @param second Second element.
   */
  public Pair(T1 first, T2 second) {
    this.first = first;
    this.second = second;
  }

  /**
   * Overrides toString() for Object.
   *
   * @return String representation of the pair.
   */
  @Override public String toString() {
    return "(" + first.toString() + ", " + second.toString() + ")";
  }
}
