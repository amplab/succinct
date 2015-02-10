package edu.berkeley.cs.succinct.regex.parser;

public class RegExUnion extends RegEx {
    private RegEx first;
    private RegEx second;

    /**
     * Constructor to initialize RegExUnion from two regular expressions.
     *  
     * @param first The first regular expression.
     * @param second The second regular expression.
     */
    public RegExUnion(RegEx first, RegEx second) {
        super(RegExType.Union);
        this.first = first;
        this.second = second;
    }

    /**
     * Get the first regular expression.
     *  
     * @return The first regular expression.
     */
    public RegEx getFirst() {
        return first;
    }

    /**
     * Get the second regular expression.
     *
     * @return The second regular expression.
     */
    public RegEx getSecond() {
        return second;
    }
}
