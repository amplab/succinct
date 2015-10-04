package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.executor.RegExExecutor;
import edu.berkeley.cs.succinct.regex.parser.RegEx;
import edu.berkeley.cs.succinct.regex.parser.RegExParser;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.regex.planner.NaiveRegExPlanner;
import edu.berkeley.cs.succinct.regex.planner.RegExPlanner;
import edu.berkeley.cs.succinct.util.Range;
import edu.berkeley.cs.succinct.util.buffers.SerializedOperations;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public class SuccinctFileBuffer extends SuccinctBuffer implements SuccinctFile {

    private static final long serialVersionUID = 5879363803993345049L;
    protected transient long fileOffset;

    /**
     * Constructor to create SuccinctBuffer from byte array, context length and file offset.
     *
     * @param input Input byte array.
     * @param contextLen Context length.
     * @param fileOffset Beginning offset for this file chunk (if file is partitioned).
     */
    public SuccinctFileBuffer(byte[] input, int contextLen, long fileOffset) {
        super(input, contextLen);
        this.fileOffset = fileOffset;
    }

    /**
     * Constructor to create SuccinctBuffer from byte array and file offset.
     *
     * @param input Input byte array.
     * @param fileOffset Beginning offset for this file chunk (if file is partitioned).
     */
    public SuccinctFileBuffer(byte[] input, long fileOffset) {
        this(input, 3, fileOffset);
    }

    /**
     * Constructor to create SuccinctBuffer from byte array.
     *
     * @param input Input byte array.
     */
    public SuccinctFileBuffer(byte[] input) {
        this(input, 0);
    }

    /**
     * Constructor to load the data from persisted Succinct data-structures.
     *
     * @param path Path to load data from.
     * @param storageMode Mode in which data is stored (In-memory or Memory-mapped)
     */
    public SuccinctFileBuffer(String path, StorageMode storageMode) {
        super(path, storageMode);
    }

    /**
     * Constructor to load the data from a ByteBuffer.
     *
     * @param buf Input buffer to load the data from
     */
    public SuccinctFileBuffer(ByteBuffer buf) {
        super(buf);
    }

    /**
     * Default constructor.
     */
    public SuccinctFileBuffer() {
        super();
    }

    /**
     * Get beginning offset for the file chunk.
     *
     * @return The beginning offset for the file chunk.
     */
    public long getFileOffset() {
        return fileOffset;
    }

    /**
     * Get offset range for the file chunk.
     *
     * @return The offset range for the file chunk.
     */
    public Range getFileRange() {
        return new Range(fileOffset, fileOffset + getOriginalSize() - 2);
    }

    /**
     * Extract data of specified length from Succinct data structures at specified index.
     *
     * @param offset Index into original input to start extracting at.
     * @param len Length of data to be extracted.
     * @return Extracted data.
     */
    @Override
    public byte[] extract(long offset, int len) {

        byte[] buf = new byte[len];
        long s;

        long chunkOffset = offset - fileOffset;
        s = lookupISA(chunkOffset);
        for (int k = 0; k < len && k < getOriginalSize(); k++) {
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
    @Override
    public byte[] extractUntil(long offset, byte delim) {

        String strBuf = "";
        long s;

        long chunkOffset = offset - fileOffset;
        s = lookupISA(chunkOffset);
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
    @Override
    public long binSearchNPA(long val, long startIdx, long endIdx, boolean flag) {

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
    @Override
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
    @Override
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
    @Override
    public Long[] search(byte[] query) {
        Range range = getRange(query);
        long sp = range.first, ep = range.second;
        if (ep - sp + 1 <= 0) {
            return new Long[0];
        }

        Long[] positions = new Long[(int)(ep - sp + 1)];
        for (long i = 0; i < ep - sp + 1; i++) {
            positions[(int)i] = lookupSA(sp + i) + fileOffset;
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
    @Override
    public Map<Long, Integer> regexSearch(String query) throws RegExParsingException {
        RegExParser parser = new RegExParser(new String(query));
        RegEx regEx;

        regEx = parser.parse();

        RegExPlanner planner = new NaiveRegExPlanner(this, regEx);
        RegEx optRegEx = planner.plan();

        RegExExecutor regExExecutor = new RegExExecutor(this, optRegEx);
        regExExecutor.execute();

        Map<Long, Integer> chunkResults = regExExecutor.getFinalResults();
        Map<Long, Integer> results = new TreeMap<Long, Integer>();
        for (Map.Entry<Long, Integer> result : chunkResults.entrySet()) {
            results.put(result.getKey() + fileOffset, result.getValue());
        }

        return results;
    }

    /**
     * Write Succinct data structures to a DataOutputStream.
     *
     * @param os Output stream to write data to.
     * @throws IOException
     */
    @Override
    public void writeToStream(DataOutputStream os) throws IOException {
        super.writeToStream(os);
        os.writeLong(fileOffset);
    }

    /**
     * Reads Succinct data structures from a DataInputStream.
     *
     * @param is Stream to read data structures from.
     * @throws IOException
     */
    @Override
    public void readFromStream(DataInputStream is) throws IOException {
        super.readFromStream(is);
        fileOffset = is.readLong();
    }

    /**
     * Reads Succinct data structures from a ByteBuffer.
     *
     * @param buf ByteBuffer to read Succinct data structures from.
     */
    @Override
    public void mapFromBuffer(ByteBuffer buf) {
        super.mapFromBuffer(buf);
        fileOffset = buf.getLong();
    }

    /**
     * Serialize SuccinctIndexedBuffer to OutputStream.
     *
     * @param oos ObjectOutputStream to write to.
     * @throws IOException
     */
    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.writeLong(fileOffset);
    }

    /**
     * Deserialize SuccinctIndexedBuffer from InputStream.
     *
     * @param ois ObjectInputStream to read from.
     * @throws IOException
     */
    private void readObject(ObjectInputStream ois)
            throws ClassNotFoundException, IOException {
        fileOffset = ois.readLong();
    }
}
