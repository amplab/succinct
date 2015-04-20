package edu.berkeley.cs.succinct.regex.parser;

public abstract class RegEx {

    private RegExType regExType;

    /**
     * Constructor to initialize RegEx with the regex type.
     *  
     * @param regExType Type of the regular expression.
     */
    public RegEx(RegExType regExType) {
        this.regExType = regExType;
    }

    /**
     * Get the type of regular expression.
     *
     * @return The regex type.
     */
    public RegExType getRegExType() {
        return regExType;
    }

}
