package edu.berkeley.cs.succinct.regex.planner;

import edu.berkeley.cs.succinct.SuccinctBuffer;
import edu.berkeley.cs.succinct.regex.parser.RegEx;

public abstract class RegExPlanner {
    protected SuccinctBuffer succinctBuffer;
    protected RegEx regEx;

    /**
     * Constructor to initialize RegExPlanner with the backing Succinct Buffer and the regex query.
     *
     * @param succinctBuffer The backing Succinct Buffer.
     * @param regEx The regular expression query.
     */
    public RegExPlanner(SuccinctBuffer succinctBuffer, RegEx regEx) {
        this.succinctBuffer = succinctBuffer;
        this.regEx = regEx;
    }

    /**
     * Generates a plan for executing the regular expression query.
     *
     * @return The regular expression execution plan.
     */
    public abstract RegEx plan();
}
