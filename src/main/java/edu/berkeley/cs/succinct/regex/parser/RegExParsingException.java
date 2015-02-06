package edu.berkeley.cs.succinct.regex.parser;

public class RegExParsingException extends Exception {

    public RegExParsingException() {
        super();
    }

    public RegExParsingException(String message) {
        super(message);
    }

    public RegExParsingException(String message, Throwable cause) {
        super(message, cause);
    }

    public RegExParsingException(Throwable cause) {
        super(cause);
    }
}
