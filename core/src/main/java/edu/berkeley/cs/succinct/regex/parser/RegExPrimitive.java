package edu.berkeley.cs.succinct.regex.parser;

public class RegExPrimitive extends RegEx {

  private String primitiveStr;
  private PrimitiveType primitiveType;
  /**
   * Constructor to initialize RegExPrimitive with primitive string.
   *
   * @param primtiveStr The string for the primitive.
   */
  public RegExPrimitive(String primtiveStr, PrimitiveType primitiveType) {
    super(RegExType.Primitive);
    this.primitiveStr = primtiveStr;
    this.primitiveType = primitiveType;
  }

  /**
   * Get the primitive type.
   *
   * @return The primitive type.
   */
  public PrimitiveType getPrimitiveType() {
    return primitiveType;
  }

  /**
   * Get primitive string.
   *
   * @return The primitive string.
   */
  public String getPrimitiveStr() {
    return primitiveStr;
  }

  public enum PrimitiveType {
    MGRAM,
    CHAR_RANGE,
    DOT
  }

}
