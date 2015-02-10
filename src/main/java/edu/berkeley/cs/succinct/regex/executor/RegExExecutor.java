package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.SuccinctBuffer;
import edu.berkeley.cs.succinct.regex.parser.*;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class RegExExecutor {

    private SuccinctBuffer succinctBuffer;
    private RegEx regEx;
    private Map<Long, Integer> finalResults;

    /**
     * Constructor to initialize Regex Executor with the Succinct Buffer and regex query.
     *
     * @param succinctBuffer The backing SuccinctBuffer.
     * @param regEx The regular expression query.
     */
    public RegExExecutor(SuccinctBuffer succinctBuffer, RegEx regEx) {
        this.succinctBuffer = succinctBuffer;
        this.regEx = regEx;
    }

    /**
     * Executes the regular expression query using the backing SuccinctBuffer.
     */
    public void execute() {
        finalResults = compute(regEx);
    }

    /**
     * Returns the set of final results.
     *
     * @return Final results for the regex query.
     */
    public Map<Long, Integer> getFinalResults() {
        return finalResults;
    }

    /**
     * Computes the regular expression query by recursively running through the regex tree.
     *
     * @param r The regular expression query.
     * @return A set of (offset, length) pairs.
     */
    private Map<Long, Integer> compute(RegEx r) {
        switch(r.getRegExType()) {
            case Blank:
            {
                return new TreeMap<Long, Integer>();
            }
            case Primitive:
            {
                return mgramSearch((RegExPrimitive)r);
            }
            case Union:
            {
                Map<Long, Integer> firstRes = compute(((RegExUnion)r).getFirst());
                Map<Long, Integer> secondRes = compute(((RegExUnion )r).getSecond());
                return regexUnion(firstRes, secondRes);
            }
            case Concat:
            {
                Map<Long, Integer> firstRes = compute(((RegExConcat)r).getFirst());
                Map<Long, Integer> secondRes = compute(((RegExConcat) r).getSecond());
                return regexConcat(firstRes, secondRes);
            }
            case Repeat:
            {
                Map<Long, Integer> internalRes = compute(((RegExRepeat) r).getInternal());
                return regexRepeat(internalRes, ((RegExRepeat)r).getRegExRepeatType());
            }
        }
        return new TreeMap<Long, Integer>();
    }

    /**
     * Computes the regular expression search results for a primitive regex query.
     *
     * @param rp Primitive regular expression.
     * @return A set of (offset, length) pairs.
     */
    protected Map<Long, Integer> mgramSearch(RegExPrimitive rp) {
        Map<Long, Integer> mgramRes = new TreeMap<Long, Integer>();
        String mgram = rp.getMgram();
        Long[] searchRes = succinctBuffer.search(mgram.getBytes());
        for(int i = 0; i < searchRes.length; i++) {
            mgramRes.put(searchRes[i], mgram.length());
        }
        return mgramRes;
    }

    /**
     * Computes the regular expression union using the results from two regex sub-expressions.
     *
     * @param a A set of (offset, length) pairs.
     * @param b A set of (offset, length) pairs.
     * @return A set of (offset, length) pairs.
     */
    protected Map<Long, Integer> regexUnion(Map<Long, Integer> a, Map<Long, Integer> b) {
        Map<Long, Integer> unionRes = new TreeMap<Long, Integer>();
        unionRes.putAll(a);
        unionRes.putAll(b);
        return unionRes;
    }

    /**
     * Computes the regex concatenation using the results from two regex sub-expressions.
     *
     * @param a A set of (offset, length) pairs.
     * @param b A set of (offset, length) pairs.
     * @return A set of (offset, length) pairs.
     */
    protected Map<Long, Integer> regexConcat(Map<Long, Integer> a, Map<Long, Integer> b) {

        Map<Long, Integer> concatRes = new TreeMap<Long, Integer>();
        Iterator<Long> bKeyIterator = b.keySet().iterator();
        for (Map.Entry<Long, Integer> entry : a.entrySet()) {
            Long curAOffset = entry.getKey();
            Integer curALength = entry.getValue();
            Long curBOffset = (long) 0;
            while(bKeyIterator.hasNext() && (curBOffset = bKeyIterator.next()) <= curAOffset) {}
            if(!bKeyIterator.hasNext()) break;
            if(curBOffset == curAOffset + curALength) {
                concatRes.put(curAOffset, curALength + b.get(curBOffset));
            }
        }
        return concatRes;
    }

    /**
     * Computes the regex repetition using the results from a regex sub-expression.
     * @param a A set of (offset, length) pairs.
     * @param repeatType The type of repeat operation.
     * @return A set of (offset, length) pairs.
     */
    protected Map<Long, Integer> regexRepeat(Map<Long, Integer> a, RegExRepeatType repeatType) {
        Map<Long, Integer> repeatRes = null;
        switch(repeatType) {
            case ZeroOrMore:
            {
                throw new UnsupportedOperationException();
            }
            case OneOrMore:
            {
                Map<Long, Integer> concatRes;
                repeatRes = concatRes = a;
                do {
                    concatRes = regexConcat(concatRes, a);
                    repeatRes.putAll(concatRes);
                } while(concatRes.size() > 0);
                break;
            }
            case MinToMax:
            {
                throw new UnsupportedOperationException();
            }
        }
        return repeatRes;
    }
}
