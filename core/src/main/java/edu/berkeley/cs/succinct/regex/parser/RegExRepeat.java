package edu.berkeley.cs.succinct.regex.parser;

public class RegExRepeat extends RegEx {
    private RegEx internal;

    private RegExRepeatType regExRepeatType;
    private int min;
    private int max;

    /**
     * Constructor to initialize RegExRepeat with input regular expression, repeat type and bounds on repetitions.
     *
     * @param internal The input regular expression.
     * @param regExRepeatType The repeat type.
     * @param min Lower bound on number of repetitions.
     * @param max Upper bound on number of repetitions.
     */
    public RegExRepeat(RegEx internal, RegExRepeatType regExRepeatType, int min, int max) {
        super(RegExType.Repeat);
        this.internal = internal;
        this.regExRepeatType = regExRepeatType;
        this.min = min;
        this.max = max;
    }

    /**
     * Constructor to initialize RegExRepeat with input regular expression and repeat type.
     *
     * @param internal The input regular expression.
     * @param regExRepeatType The repeat type.
     */
    public RegExRepeat(RegEx internal, RegExRepeatType regExRepeatType) {
        this(internal, regExRepeatType, -1, -1);
    }

    /**
     * Get internal regular expression.
     *  
     * @return The internal regular expression.
     */
    public RegEx getInternal() {
        return internal;
    }

    /**
     * Get the repeat type.
     *
     * @return The repeat type.
     */
    public RegExRepeatType getRegExRepeatType() {
        return regExRepeatType;
    }

    /**
     * Get the lower bound on number of repetitions.
     *
     * @return The lower bound on number of repetitions.
     */
    public int getMin() {
        return min;
    }

    /**
     * Get the upper bound on number of repetitions.
     *
     * @return The upper bound on number of repetitions.
     */
    public int getMax() {
        return max;
    }
}
