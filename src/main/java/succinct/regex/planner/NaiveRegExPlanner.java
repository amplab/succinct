package succinct.regex.planner;

import succinct.SuccinctBuffer;
import succinct.regex.parser.RegEx;

public class NaiveRegExPlanner extends RegExPlanner {

    public NaiveRegExPlanner(SuccinctBuffer succinctBuffer, RegEx regEx) {
        super(succinctBuffer, regEx);
    }

    @Override
    public RegEx plan() {
        return regEx;
    }
}
