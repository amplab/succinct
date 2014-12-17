package succinct.regex.parser;

public class RegExRepeat extends RegEx {
    private RegEx internal;

    private RegExRepeatType regExRepeatType;
    private int min;
    private int max;

    public RegExRepeat(RegEx internal, RegExRepeatType regExRepeatType, int min, int max) {
        super(RegExType.Repeat);
        this.internal = internal;
        this.regExRepeatType = regExRepeatType;
        this.min = min;
        this.max = max;
    }

    public RegExRepeat(RegEx internal, RegExRepeatType regExRepeatType) {
        this(internal, regExRepeatType, -1, -1);
    }

    public RegEx getInternal() {
        return internal;
    }

    public RegExRepeatType getRegExRepeatType() {
        return regExRepeatType;
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }
}
