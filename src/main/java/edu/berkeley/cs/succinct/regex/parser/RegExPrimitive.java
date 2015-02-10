package edu.berkeley.cs.succinct.regex.parser;

public class RegExPrimitive extends RegEx {

    private String mgram;

    /**
     * Constructor to initialize RegExPrimitive with multigram.
     *
     * @param mgram Input multigram.
     */
    public RegExPrimitive(String mgram) {
        super(RegExType.Primitive);
        this.mgram = mgram;
    }

    /**
     * Get multigram.
     *
     * @return The multigram.
     */
    public String getMgram() {
        return mgram;
    }

}
