package edu.berkeley.cs.succinct.regex.parser;

public class RegExConcat extends RegEx {

    RegEx first;
    RegEx second;

    public RegExConcat(RegEx first, RegEx second) {
        super(RegExType.Concat);
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
