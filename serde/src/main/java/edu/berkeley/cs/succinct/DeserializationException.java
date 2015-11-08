package edu.berkeley.cs.succinct;

public class DeserializationException extends Exception {

  public DeserializationException(String message) {
    super(message);
  }

  public DeserializationException(Throwable cause) {
    super(cause);
  }
}
