package edu.berkeley.cs.succinct.regex.planner;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.parser.RegEx;

public abstract class RegExPlanner {
    protected SuccinctFile succinctFile;
    protected RegEx regEx;

    /**
     * Constructor to initialize RegExPlanner with the backing Succinct Buffer and the regex query.
     *
     * @param succinctFile The backing Succinct Buffer.
     * @param regEx The regular expression query.
     */
    public RegExPlanner(SuccinctFile succinctFile, RegEx regEx) {
        this.succinctFile = succinctFile;
        this.regEx = regEx;
    }

    /**
     * Generates a plan for executing the regular expression query.
     *
     * @return The regular expression execution plan.
     */
    public abstract RegEx plan();
}
