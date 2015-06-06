package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.dictionary.Tables;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class SuccinctIndexedBuffer extends SuccinctBuffer {

    private static final long serialVersionUID = -8357331195541317163L;

    protected transient static byte RECORD_DELIM = '\n';
    protected transient int[] offsets;

    /**
     * Constructor to initialize SuccinctIndexedBuffer from input byte array, offsets corresponding to records, and
     * context length.
     *
     * @param input The input byte array.
     * @param offsets Offsets corresponding to records.
     * @param contextLen Context Length.
     */
    public SuccinctIndexedBuffer(byte[] input, int[] offsets, int contextLen) {
        super(input, contextLen);
        this.offsets = offsets;
    }

    /**
     * Constructor to initialize SuccinctIndexedBuffer from input byte array and offsets corresponding to records
     *
     * @param input The input byte array.
     * @param offsets Offsets corresponding to records.
     */
    public SuccinctIndexedBuffer(byte[] input, int[] offsets) {
        this(input, offsets, 3);
    }

    /**
     * Constructor to load the data from persisted Succinct data-structures.
     *
     * @param path Path to load data from.
     * @param storageMode Mode in which data is stored (In-memory or Memory-mapped)
     */
    public SuccinctIndexedBuffer(String path, StorageMode storageMode) {
        this.storageMode = storageMode;
        try {
            Tables.init();
            if (storageMode == StorageMode.MEMORY_ONLY) {
                readFromFile(path);
            } else if (storageMode == StorageMode.MEMORY_MAPPED) {
                memoryMap(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Constructor to load the data from a DataInputStream.
     *
     * @param is Input stream to load the data from
     */
    public SuccinctIndexedBuffer(DataInputStream is) {
        try {
            Tables.init();
            readFromStream(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Constructor to load the data from a ByteBuffer.
     *
     * @param buf Input buffer to load the data from
     */
    public SuccinctIndexedBuffer(ByteBuffer buf) {
        Tables.init();
        mapFromBuffer(buf);
    }

    /**
     * Get the record delimiter.
     *
     * @return The record delimiter.
     */
    public static byte getRecordDelim() {
        return RECORD_DELIM;
    }

    /**
     * Get the number of records.
     *
     * @return The number of records.
     */
    public int getNumRecords() {
        return offsets.length;
    }

    /**
     * Get the ith record.
     *
     * @param i The record index.
     * @return The corresponding record.
     */
    public byte[] getRecord(int i) {
        int length;
        if(i == offsets.length - 1) {
            length = (int)(getOriginalSize() - 2 - offsets[i]);
        } else {
            length = (int)(offsets[i + 1] - 1 - offsets[i]);
        }
        return extract((int) offsets[i], length);
    }

    /**
     * Search for offset corresponding to a position in the input.
     *
     * @param pos Position in the input
     * @return Offset corresponding to the position.
     */
    private int searchOffset(long pos) {
        int sp = 0, ep = offsets.length - 1;
        int m;

        while (sp <= ep) {
            m = (sp + ep) / 2;
            if (offsets[m] == pos) {
                return m;
            } else if (pos < offsets[m]) {
                ep = m - 1;
            } else {
                sp = m + 1;
            }
        }

        return ep;
    }

    /**
     * Search for an input query and return offsets of all matching records.
     *
     * @param query Input query.
     * @return Offsets of all matching records.
     */
    public Integer[] recordSearchOffsets(byte[] query) {
        Set<Integer> results = new HashSet<Integer>();
        Range range = getRange(query);

        long sp = range.first, ep = range.second;
        if (ep - sp + 1 <= 0) {
            return new Integer[0];
        }

        for (long i = 0; i < ep - sp + 1; i++) {
            results.add(offsets[searchOffset(lookupSA(sp + i))]);
        }

        return results.toArray(new Integer[results.size()]);
    }

    /**
     * Search for all records that contains the query.
     *
     * @param query Input query.
     * @return All records containing input query.
     */
    public byte[][] recordSearch(byte[] query) {
        Set<Long> offsetResults = new HashSet<Long>();
        ArrayList<byte[]> results = new ArrayList<byte[]>();
        Range range = getRange(query);

        long sp = range.first, ep = range.second;
        if (ep - sp + 1 <= 0) {
            return new byte[0][0];
        }

        for (long i = 0; i < ep - sp + 1; i++) {
            long saVal = lookupSA(sp + i);
            int offsetIdx = searchOffset(saVal);
            long offset = offsets[offsetIdx];
            if(!offsetResults.contains(offset)) {
                results.add(extractUntil((int) offset, RECORD_DELIM));
                offsetResults.add(offset);
            }

        }

        return results.toArray(new byte[results.size()][]);
    }

    /**
     * Performs a range search for all records that contains a substring between queryBegin and queryEnd.
     *
     * @param queryBegin The beginning of query range.
     * @param queryEnd The end of query range.
     * @return All records matching the query range.
     */
    public byte[][] recordRangeSearch(byte[] queryBegin, byte[] queryEnd) {
        Set<Long> offsetResults = new HashSet<Long>();
        ArrayList<byte[]> results = new ArrayList<byte[]>();
        Range rangeBegin = getRange(queryBegin);
        Range rangeEnd = getRange(queryEnd);

        long sp = rangeBegin.first, ep = rangeEnd.second;
        if (ep - sp + 1 <= 0) {
            return new byte[0][0];
        }

        for (long i = 0; i < ep - sp + 1; i++) {
            long saVal = lookupSA(sp + i);
            int offsetIdx = searchOffset(saVal);
            long offset = offsets[offsetIdx];
            if(!offsetResults.contains(offset)) {
                results.add(extractUntil((int) offset, RECORD_DELIM));
                offsetResults.add(offset);
            }

        }

        return results.toArray(new byte[results.size()][]);
    }

    /**
     * Get the union of multiple overlapping ranges.
     *
     * @param ranges List of ranges.
     * @return The union of the list of ranges.
     */
    private ArrayList<Range> unionRanges(ArrayList<Range> ranges) {
        if(ranges.size() <= 1)
            return ranges;

        Stack<Range> rangeStack = new Stack<Range>();

        Collections.sort(ranges);
        rangeStack.push(ranges.get(0));

        for(int i = 0; i < ranges.size(); i++) {
            Range top = rangeStack.peek();
            if(top.second < ranges.get(i).first) {
                rangeStack.push(ranges.get(i));
            } else if(top.second < ranges.get(i).second) {
                top.second = ranges.get(i).second;
                rangeStack.pop();
                rangeStack.push(top);
            }
        }

        return new ArrayList<Range>(rangeStack);
    }

    /**
     * Perform multiple searches and return the union of the results.
     *
     * @param queries The list of queries.
     * @return The records matching the multi-search queries.
     */
    public byte[][] multiSearchUnion(byte[][] queries) {
        Set<Long> offsetResults = new HashSet<Long>();
        ArrayList<byte[]> results = new ArrayList<byte[]>();

        // Get all ranges
        ArrayList<Range> ranges = new ArrayList<Range>();
        for (int qid = 0; qid < queries.length; qid++) {
            Range range = getRange(queries[qid]);
            if (range.second - range.first + 1 > 0) {
                ranges.add(range);
            }
        }

        // Union of all ranges
        ranges = unionRanges(ranges);

        for (Range range : ranges) {
            long sp = range.first, ep = range.second;

            for (long i = 0; i < ep - sp + 1; i++) {
                long saVal = lookupSA(sp + i);
                int offsetIdx = searchOffset(saVal);
                long offset = offsets[offsetIdx];
                if (!offsetResults.contains(offset)) {
                    results.add(extractUntil((int) offset, RECORD_DELIM));
                    offsetResults.add(offset);
                }
            }
        }

        return results.toArray(new byte[results.size()][]);
    }

    class RangeSizeComparator implements Comparator<Range> {
        @Override
        public int compare(Range r1, Range r2) {
            return (int)((r1.second - r1.first) - (r2.second - r2.first));
        }
    }

    /**
     * Perform multiple searches and return the intersection of the results.
     *
     * @param queries The list of queries.
     * @return The records matching the multi-search queries.
     */
    public byte[][] multiSearchIntersect(byte[][] queries) {
        Set<Long> offsetResults = new TreeSet<Long>();
        ArrayList<byte[]> results = new ArrayList<byte[]>();

        // Get all ranges
        ArrayList<Range> ranges = new ArrayList<Range>();
        for (int qid = 0; qid < queries.length; qid++) {
            Range range = getRange(queries[qid]);
            if (range.second - range.first + 1 > 0) {
                ranges.add(range);
            } else {
                return new byte[0][0];
            }
        }

        Collections.sort(ranges, new RangeSizeComparator());

        // Populate the set of offsets corresponding to the first range
        Range firstRange = ranges.get(0);
        Map<Long, Long> counts = new HashMap<Long, Long>();
        {
            long sp = firstRange.first, ep = firstRange.second;
            for (long i = 0; i < ep - sp + 1; i++) {
                long saVal = lookupSA(sp + i);
                int offsetIdx = searchOffset(saVal);
                long offset = offsets[offsetIdx];
                offsetResults.add(offset);
                counts.put(offset, 1L);
            }
        }

        ranges.remove(firstRange);
        for(Range range: ranges) {
            long sp = range.first, ep = range.second;

            for (long i = 0; i < ep - sp + 1; i++) {
                long saVal = lookupSA(sp + i);
                int offsetIdx = searchOffset(saVal);
                long offset = offsets[offsetIdx];
                if(offsetResults.contains(offset)) {
                    counts.put(offset, counts.get(offset) + 1);
                }
            }
        }

        for(Long offset: offsetResults) {
            if(counts.get(offset) == queries.length) {
                results.add(extractUntil(offset.intValue(), RECORD_DELIM));
            }
        }

        return results.toArray(new byte[results.size()][]);
    }

    /**
     * Defines the types of search queries that SuccinctIndexedBuffer can handle in a multiSearch.
     */
    public enum QueryType {
        Search,
        RangeSearch
        /* TODO: Add more */
    }

    /**
     * Perform multiple searches with different query types and return the intersection of the results.
     *
     * @param queryTypes The QueryType corresponding to each query
     * @param queries The actual query parameters associated with each query
     * @return The records matching the multi-search queries.
     */
    public byte[][] multiSearch(QueryType[] queryTypes, byte[][][] queries) {
        assert(queryTypes.length == queries.length);
        Set<Long> offsetResults = new TreeSet<Long>();
        ArrayList<byte[]> results = new ArrayList<byte[]>();

        if(queries.length == 0) {
            throw new IllegalArgumentException("multiSearch called with empty queries");
        }

        // Get all ranges
        ArrayList<Range> ranges = new ArrayList<Range>();
        for (int qid = 0; qid < queries.length; qid++) {
            Range range;

            switch (queryTypes[qid]) {
                case Search:
                {
                    range = getRange(queries[qid][0]);
                    break;
                }
                case RangeSearch:
                {
                    byte[] queryBegin = queries[qid][0];
                    byte[] queryEnd = queries[qid][1];
                    Range rangeBegin, rangeEnd;
                    rangeBegin = getRange(queryBegin);
                    rangeEnd = getRange(queryEnd);
                    range = new Range(rangeBegin.first, rangeEnd.second);
                    break;
                }
                default:
                {
                    throw new UnsupportedOperationException("Unsupported QueryType");
                }
            }

            if (range.second - range.first + 1 > 0) {
                ranges.add(range);
            } else {
                return new byte[0][0];
            }
        }
        int numRanges = ranges.size();

        Collections.sort(ranges, new RangeSizeComparator());

        // Populate the set of offsets corresponding to the first range
        Range firstRange = ranges.get(0);
        Map<Long, Long> counts = new HashMap<Long, Long>();
        {
            long sp = firstRange.first, ep = firstRange.second;
            for (long i = 0; i < ep - sp + 1; i++) {
                long saVal = lookupSA(sp + i);
                int offsetIdx = searchOffset(saVal);
                long offset = offsets[offsetIdx];
                offsetResults.add(offset);
                counts.put(offset, 1L);
            }
        }

        ranges.remove(firstRange);
        for (Range range: ranges) {
            long sp = range.first, ep = range.second;

            for (long i = 0; i < ep - sp + 1; i++) {
                long saVal = lookupSA(sp + i);
                int offsetIdx = searchOffset(saVal);
                long offset = offsets[offsetIdx];
                if (offsetResults.contains(offset)) {
                    counts.put(offset, counts.get(offset) + 1);
                }
            }
        }

        for (Long offset: offsetResults) {
            if (counts.get(offset) == numRanges) {
                results.add(extractUntil(offset.intValue(), RECORD_DELIM));
            }
        }

        return results.toArray(new byte[results.size()][]);
    }

    /**
     * Count of all records containing a particular query.
     *
     * @param query Input query.
     * @return Count of all records containing input query.
     */
    public long recordCount(byte[] query) {
        return recordSearchOffsets(query).length;
    }

    /**
     * Extract a part of all records.
     *
     * @param offset Offset into record.
     * @param length Length of part to be extracted.
     * @return Extracted data.
     */
    public byte[][] extractRecords(int offset, int length) {
        byte[][] records = new byte[offsets.length][];
        for(int i = 0; i < records.length; i++) {
            long curOffset = offsets[i] + offset;
            long nextOffset = (i == records.length - 1) ? getOriginalSize() : offsets[i + 1];
            if(length < nextOffset - curOffset)
                records[i] = extract((int) curOffset, length);
            else
                records[i] = extractUntil((int) curOffset, RECORD_DELIM);
        }
        return records;
    }

    /**
     * Search for all records that contain a particular regular expression.
     *
     * @param query The regular expression (UTF-8 encoded).
     * @return The records that contain the regular search expression.
     * @throws RegExParsingException
     */
    public byte[][] recordSearchRegex(String query) throws RegExParsingException {
        Map<Long, Integer> regexOffsetResults = regexSearch(query);
        Set<Long> offsetResults = new HashSet<Long>();
        ArrayList<byte[]> results = new ArrayList<byte[]>();
        for(Long offset: regexOffsetResults.keySet()) {
            int offsetIdx = searchOffset(offset);
            long recordOffset = offsets[offsetIdx];
            if(!offsetResults.contains(recordOffset)) {
                results.add(extractUntil((int)recordOffset, RECORD_DELIM));
                offsetResults.add(recordOffset);
            }
        }
        return results.toArray(new byte[results.size()][]);
    }

    /**
     * Write Succinct data structures to a DataOutputStream.
     *
     * @param os Output stream to write data to.
     * @throws IOException
     */
    public void writeToStream(DataOutputStream os) throws IOException {
        super.writeToStream(os);
        os.writeInt(offsets.length);
        for(int i = 0; i < offsets.length; i++) {
            os.writeInt(offsets[i]);
        }
    }

    /**
     * Reads Succinct data structures from a DataInputStream.
     *
     * @param is Stream to read data structures from.
     * @throws IOException
     */
    public void readFromStream(DataInputStream is) throws IOException {
        super.readFromStream(is);
        int len = is.readInt();
        offsets = new int[len];
        for(int i = 0; i < len; i++) {
            offsets[i] = is.readInt();
        }
    }

    /**
     * Reads Succinct data structures from a ByteBuffer.
     *
     * @param buf ByteBuffer to read Succinct data structures from.
     */
    public void mapFromBuffer(ByteBuffer buf) {
        super.mapFromBuffer(buf);
        int len = buf.getInt();
        offsets = new int[len];
        for(int i = 0; i < offsets.length; i++) {
            offsets[i] = buf.getInt();
        }
    }

    /**
     * Convert Succinct data-structures to a byte array.
     *
     * @return Byte array containing serialzied Succinct data structures.
     * @throws IOException
     */
    public byte[] toByteArray() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        writeToStream(new DataOutputStream(bos));
        return bos.toByteArray();
    }

    /**
     * Read Succinct data structures from byte array.
     *
     * @param data Byte array to read data from.
     * @throws IOException
     */
    public void fromByteArray(byte[] data) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        readFromStream(new DataInputStream(bis));
    }

    /**
     * Write Succinct data structures to file.
     *
     * @param path Path to file where Succinct data structures should be written.
     * @throws IOException
     */
    public void writeToFile(String path) throws IOException {
        FileOutputStream fos = new FileOutputStream(path);
        DataOutputStream os = new DataOutputStream(fos);
        writeToStream(os);
    }

    /**
     * Read Succinct data structures into memory from file.
     *
     * @param path Path to serialized Succinct data structures.
     * @throws IOException
     */
    public void readFromFile(String path) throws IOException {
        FileInputStream fis = new FileInputStream(path);
        DataInputStream is = new DataInputStream(fis);
        readFromStream(is);
    }

    /**
     * Memory maps serialized Succinct data structures.
     *
     * @param path Path to serialized Succinct data structures.
     * @throws IOException
     */
    public void memoryMap(String path) throws IOException {
        File file = new File(path);
        long size = file.length();
        FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();

        ByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
        mapFromBuffer(buf);
        offsets = new int[buf.getInt()];
        buf.asIntBuffer().get(offsets);
    }

    /**
     * Serialize SuccinctIndexedBuffer to OutputStream.
     *
     * @param oos ObjectOutputStream to write to.
     * @throws IOException
     */
    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.writeObject(offsets);
    }

    /**
     * Deserialize SuccinctIndexedBuffer from InputStream.
     *
     * @param ois ObjectInputStream to read from.
     * @throws IOException
     */
    private void readObject(ObjectInputStream ois)
            throws ClassNotFoundException, IOException {
        offsets = (int [])ois.readObject();
    }
}
