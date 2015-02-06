package edu.berkeley.cs.succinct.regex.planner;

import edu.berkeley.cs.succinct.SuccinctBuffer;
import edu.berkeley.cs.succinct.regex.parser.RegEx;

public abstract class RegExPlanner {
    protected SuccinctBuffer succinctBuffer;
    protected RegEx regEx;

    public RegExPlanner(SuccinctBuffer succinctBuffer, RegEx regEx) {
        this.succinctBuffer = succinctBuffer;
        this.regEx = regEx;
    }

    public abstract RegEx plan();
}
