package edu.berkeley.cs.succinct.regex.parser;

public abstract class RegEx {

    private RegExType regExType;
    public RegEx(RegExType regExType) {
        this.regExType = regExType;
    }

    public RegExType getRegExType() {
        return regExType;
    }

}
