package edu.berkeley.cs.succinct;

public enum DataType {
  /* Primitives */
  BOOLEAN(0),
  BYTE(1),
  SHORT(2),
  INT(3),
  LONG(4),
  FLOAT(5),
  DOUBLE(6),
  STRING(7);

  private final int order;

  DataType(int order) {
    this.order = order;
  }

  public int getOrder() {
    return order;
  }

  public static DataType encapsulatingType(DataType type1, DataType type2) {
    if (type1.getOrder() < type2.getOrder()) {
      return type2;
    }
    return type1;
  }
}
