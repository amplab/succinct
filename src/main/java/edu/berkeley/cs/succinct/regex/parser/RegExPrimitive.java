package edu.berkeley.cs.succinct.regex.parser;

public class RegExPrimitive extends RegEx {

    private String mgram;

    /**
     * Constructor to initialize RegExPrimitive with multi-gram.
     *
     * @param mgram Input multi-gram.
     */
    public RegExPrimitive(String mgram) {
        super(RegExType.Primitive);
        this.mgram = mgram;
    }

    /**
     * Get multi-gram.
     *
     * @return The multi-gram.
     */
    public String getMgram() {
        return mgram;
    }

}
