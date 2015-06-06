package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.dictionary.Tables;
import edu.berkeley.cs.succinct.regex.executor.RegExExecutor;
import edu.berkeley.cs.succinct.regex.parser.RegEx;
import edu.berkeley.cs.succinct.regex.parser.RegExParser;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.regex.planner.NaiveRegExPlanner;
import edu.berkeley.cs.succinct.regex.planner.RegExPlanner;
import edu.berkeley.cs.succinct.util.SerializedOperations;

import java.util.Map;

public class SuccinctBuffer extends SuccinctCore {

    private static final long serialVersionUID = 5879363803993345049L;

    /**
     * Constructor to create SuccinctBuffer from byte array and context length.
     *
     * @param input Input byte array.
     * @param contextLen Context length.
     */
    public SuccinctBuffer(byte[] input, int contextLen) {
        super(input, contextLen);
    }

    /**
     * Constructor to create SuccinctBuffer from byte array.
     *
     * @param input Input byte array.
     */
    public SuccinctBuffer(byte[] input) {
        this(input, 3);
    }

    /**
     * Constructor to load the data from persisted Succinct data-structures.
     *
     * @param path Path to load data from.
     * @param storageMode Mode in which data is stored (In-memory or Memory-mapped)
     */
    public SuccinctBuffer(String path, StorageMode storageMode) {
        super(path, storageMode);
    }

    /**
     * Default constructor.
     */
    public SuccinctBuffer() {
        super();
    }

    /**
     * Extract data of specified length from Succinct data structures at specified index.
     *
     * @param offset Index into original input to start extracting at.
     * @param len Length of data to be extracted.
     * @return Extracted data.
     */
    public byte[] extract(int offset, int len) {

        byte[] buf = new byte[len];
        long s;

        s = lookupISA(offset);
        for (int k = 0; k < len; k++) {
            buf[k] = alphabet.get(SerializedOperations.ArrayOps.getRank1(
                    coloffsets.buffer(), 0, getSigmaSize(), s) - 1);
            s = lookupNPA(s);
        }

        return buf;
    }

    /**
     * Extract data from Succinct data structures at specified index until specified delimiter.
     *
     * @param offset Index into original input to start extracting at.
     * @param delim Delimiter at which to stop extracting.
     * @return Extracted data.
     */
    public byte[] extractUntil(int offset, byte delim) {

        String strBuf = "";
        long s;

        s = lookupISA(offset);
        char nextChar;
        do {
            nextChar = (char) alphabet.get(SerializedOperations.ArrayOps.getRank1(
                    coloffsets.buffer(), 0, getSigmaSize(), s) - 1);
            if(nextChar == delim || nextChar == 1) break;
            strBuf += nextChar;
            s = lookupNPA(s);
        } while(true);

        return strBuf.getBytes();
    }

    /**
     * Binary Search for a value withing NPA.
     *
     * @param val Value to be searched.
     * @param startIdx Starting index into NPA.
     * @param endIdx Ending index into NPA.
     * @param flag Whether to search for left or the right boundary.
     * @return Search result as an index into the NPA.
     */
    private long binSearchNPA(long val, long startIdx, long endIdx, boolean flag) {

        long sp = startIdx;
        long ep = endIdx;
        long m;

        while (sp <= ep) {
            m = (sp + ep) / 2;

            long psi_val;
            psi_val = lookupNPA(m);

            if (psi_val == val) {
                return m;
            } else if (val < psi_val) {
                ep = m - 1;
            } else {
                sp = m + 1;
            }
        }

        return flag ? ep : sp;
    }

    /**
     * Get range of SA positions using Backward search
     *
     * @param buf Input query to be searched.
     * @return Range of indices into the SA.
     */
    public Range getRange(byte[] buf) {
        Range range = new Range(0L, -1L);
        int m = buf.length;
        long c1, c2;

        if (alphabetMap.containsKey(buf[m - 1])) {
            range.first = alphabetMap.get(buf[m - 1]).first;
            range.second = alphabetMap.get((alphabet.get(alphabetMap.get(buf[m - 1]).second + 1))).first - 1;
        } else {
            return range;
        }

        for (int i = m - 2; i >= 0; i--) {
            if (alphabetMap.containsKey(buf[i])) {
                c1 = alphabetMap.get(buf[i]).first;
                c2 = alphabetMap.get((alphabet.get(alphabetMap.get(buf[i]).second + 1))).first - 1;
            } else {
                return range;
            }
            range.first = binSearchNPA(range.first, c1, c2, false);
            range.second = binSearchNPA(range.second, c1, c2, true);
        }

        return range;
    }

    /**
     * Get count of pattern occurrences in original input.
     *
     * @param query Input query.
     * @return Count of occurrences.
     */
    public long count(byte[] query) {
        Range range = getRange(query);
        return range.second - range.first + 1;
    }

    /**
     * Get all locations of pattern occurrences in original input.
     *
     * @param query Input query.
     * @return All locations of pattern occurrences in original input.
     */
    public Long[] search(byte[] query) {
        Range range = getRange(query);
        long sp = range.first, ep = range.second;
        if (ep - sp + 1 <= 0) {
            return new Long[0];
        }

        Long[] positions = new Long[(int)(ep - sp + 1)];
        for (long i = 0; i < ep - sp + 1; i++) {
            positions[(int)i] = lookupSA(sp + i);
        }

        return positions;
    }

    /**
     * Performs regular expression search for an input expression using Succinct data-structures.
     *
     * @param query Regular expression pattern to be matched. (UTF-8 encoded)
     * @return All locations and lengths of matching patterns in original input.
     * @throws RegExParsingException
     */
    public Map<Long, Integer> regexSearch(String query) throws RegExParsingException {
        RegExParser parser = new RegExParser(new String(query));
        RegEx regEx;

        regEx = parser.parse();

        RegExPlanner planner = new NaiveRegExPlanner(this, regEx);
        RegEx optRegEx = planner.plan();

        RegExExecutor regExExecutor = new RegExExecutor(this, optRegEx);
        regExExecutor.execute();

        return regExExecutor.getFinalResults();
    }
}
