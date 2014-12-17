package succinct.regex.planner;

import succinct.SuccinctBuffer;
import succinct.regex.parser.RegEx;

public abstract class RegExPlanner {
    protected SuccinctBuffer succinctBuffer;
    protected RegEx regEx;

    public RegExPlanner(SuccinctBuffer succinctBuffer, RegEx regEx) {
        this.succinctBuffer = succinctBuffer;
        this.regEx = regEx;
    }

    public abstract RegEx plan();
}
