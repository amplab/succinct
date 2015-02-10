package edu.berkeley.cs.succinct.regex.planner;

import edu.berkeley.cs.succinct.SuccinctBuffer;
import edu.berkeley.cs.succinct.regex.parser.RegEx;

public class NaiveRegExPlanner extends RegExPlanner {

    /**
     * Constructor to initialize NaiveRegExPlanner with the backing Succinct Buffer and the regex query.
     *
     * @param succinctBuffer The backing Succinct Buffer.
     * @param regEx The regex query.
     */
    public NaiveRegExPlanner(SuccinctBuffer succinctBuffer, RegEx regEx) {
        super(succinctBuffer, regEx);
    }

    /**
     * Generates a plan for regular expression execution.
     * The naive planner simply returns the original regular expression.
     *  
     * @return The regex execution plan.
     */
    @Override
    public RegEx plan() {
        return regEx;
    }
}
