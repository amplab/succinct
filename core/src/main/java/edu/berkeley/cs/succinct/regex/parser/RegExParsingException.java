package edu.berkeley.cs.succinct.regex.parser;

public class RegExParsingException extends Exception {

  /**
   * Default constructor.
   */
  public RegExParsingException() {
    super();
  }

  /**
   * Constructor to initialize RegExParsingExpression with message.
   *
   * @param message The exception message.
   */
  public RegExParsingException(String message) {
    super(message);
  }

  /**
   * Constructor to initialize RegExParsingExpression with message and cause.
   *
   * @param message The exception message.
   * @param cause   The exception cause.
   */
  public RegExParsingException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructor to initialize RegExParsingExpression with cause.
   *
   * @param cause The exception cause.
   */
  public RegExParsingException(Throwable cause) {
    super(cause);
  }
}
