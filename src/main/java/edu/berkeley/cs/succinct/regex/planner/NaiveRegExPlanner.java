package edu.berkeley.cs.succinct.regex.planner;

import edu.berkeley.cs.succinct.SuccinctBuffer;
import edu.berkeley.cs.succinct.regex.parser.RegEx;

public class NaiveRegExPlanner extends RegExPlanner {

    public NaiveRegExPlanner(SuccinctBuffer succinctBuffer, RegEx regEx) {
        super(succinctBuffer, regEx);
    }

    @Override
    public RegEx plan() {
        return regEx;
    }
}
