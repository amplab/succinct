package edu.berkeley.cs.succinct.regex.parser;

public class RegExUnion extends RegEx {
    private RegEx first;
    private RegEx second;

    public RegExUnion(RegEx first, RegEx second) {
        super(RegExType.Union);
        this.first = first;
        this.second = second;
    }

    public RegEx getFirst() {
        return first;
    }

    public RegEx getSecond() {
        return second;
    }
}
