package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.util.Range;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public interface SuccinctFile extends Serializable {

    /**
     * Extract data of specified length from Succinct data structures at specified index.
     *
     * @param offset Index into original input to start extracting at.
     * @param len Length of data to be extracted.
     * @return Extracted data.
     */
    byte[] extract(int offset, int len);

    /**
     * Extract data from Succinct data structures at specified index until specified delimiter.
     *
     * @param offset Index into original input to start extracting at.
     * @param delim Delimiter at which to stop extracting.
     * @return Extracted data.
     */
    byte[] extractUntil(int offset, byte delim);

    /**
     * Binary Search for a value withing NPA.
     *
     * @param val Value to be searched.
     * @param startIdx Starting index into NPA.
     * @param endIdx Ending index into NPA.
     * @param flag Whether to search for left or the right boundary.
     * @return Search result as an index into the NPA.
     */
    long binSearchNPA(long val, long startIdx, long endIdx, boolean flag);

    /**
     * Get range of SA positions using Backward search
     *
     * @param buf Input query to be searched.
     * @return Range of indices into the SA.
     */
    Range getRange(byte[] buf);

    /**
     * Get count of pattern occurrences in original input.
     *
     * @param query Input query.
     * @return Count of occurrences.
     */
    long count(byte[] query);


    /**
     * Get all locations of pattern occurrences in original input.
     *
     * @param query Input query.
     * @return All locations of pattern occurrences in original input.
     */
    Long[] search(byte[] query);

    /**
     * Performs regular expression search for an input expression using Succinct data-structures.
     *
     * @param query Regular expression pattern to be matched. (UTF-8 encoded)
     * @return All locations and lengths of matching patterns in original input.
     * @throws RegExParsingException
     */
    Map<Long, Integer> regexSearch(String query) throws RegExParsingException;

    /**
     * Reads Succinct data structures from a DataInputStream.
     *
     * @param is Stream to read data structures from.
     * @throws IOException
     */
    void readFromStream(DataInputStream is) throws IOException;

    /**
     * Write Succinct data structures to a DataOutputStream.
     *
     * @param os Output stream to write data to.
     * @throws IOException
     */
    void writeToStream(DataOutputStream os) throws IOException;


}
