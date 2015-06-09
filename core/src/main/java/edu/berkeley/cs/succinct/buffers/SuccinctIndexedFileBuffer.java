package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctIndexedFile;
import edu.berkeley.cs.succinct.dictionary.Tables;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.util.Range;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class SuccinctIndexedFileBuffer extends SuccinctFileBuffer implements SuccinctIndexedFile {

    private static final long serialVersionUID = -8357331195541317163L;

    protected transient int[] offsets;

    /**
     * Constructor to initialize SuccinctIndexedBuffer from input byte array, offsets corresponding to records, and
     * context length.
     *
     * @param input The input byte array.
     * @param offsets Offsets corresponding to records.
     * @param contextLen Context Length.
     */
    public SuccinctIndexedFileBuffer(byte[] input, int[] offsets, int contextLen) {
        super(input, contextLen);
        this.offsets = offsets;
    }

    /**
     * Constructor to initialize SuccinctIndexedBuffer from input byte array and offsets corresponding to records
     *
     * @param input The input byte array.
     * @param offsets Offsets corresponding to records.
     */
    public SuccinctIndexedFileBuffer(byte[] input, int[] offsets) {
        this(input, offsets, 3);
    }

    /**
     * Constructor to load the data from persisted Succinct data-structures.
     *
     * @param path Path to load data from.
     * @param storageMode Mode in which data is stored (In-memory or Memory-mapped)
     */
    public SuccinctIndexedFileBuffer(String path, StorageMode storageMode) {
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
    public SuccinctIndexedFileBuffer(DataInputStream is) {
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
    public SuccinctIndexedFileBuffer(ByteBuffer buf) {
        Tables.init();
        mapFromBuffer(buf);
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
    @Override
    public byte[] getRecord(int i) {
        if(i >= offsets.length || i < 0) {
            throw new ArrayIndexOutOfBoundsException("Record does not exist: i = " + i);
        }
        int begOffset = offsets[i];
        int endOffset = (i == offsets.length - 1) ? getOriginalSize() - 1 : offsets[i + 1];
        int len = (endOffset - begOffset - 1);
        return extract(begOffset, len);
    }

    /**
     * Search for offset corresponding to a position in the input.
     *
     * @param pos Position in the input
     * @return Offset corresponding to the position.
     */
    @Override
    public int searchOffset(int pos) {
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
    @Override
    public Integer[] recordSearchOffsets(byte[] query) {
        Set<Integer> results = new HashSet<Integer>();
        Range range = getRange(query);

        long sp = range.first, ep = range.second;
        if (ep - sp + 1 <= 0) {
            return new Integer[0];
        }

        for (long i = 0; i < ep - sp + 1; i++) {
            results.add(offsets[searchOffset((int) lookupSA(sp + i))]);
        }

        return results.toArray(new Integer[results.size()]);
    }

    /**
     * Count of all records containing a particular query.
     *
     * @param query Input query.
     * @return Count of all records containing input query.
     */
    @Override
    public long recordCount(byte[] query) {
        return recordSearchOffsets(query).length;
    }

    /**
     * Search for all records that contains the query.
     *
     * @param query Input query.
     * @return All records containing input query.
     */
    @Override
    public byte[][] recordSearch(byte[] query) {
        Set<Integer> recordIds = new HashSet<Integer>();
        ArrayList<byte[]> results = new ArrayList<byte[]>();
        Range range = getRange(query);

        long sp = range.first, ep = range.second;
        if (ep - sp + 1 <= 0) {
            return new byte[0][0];
        }

        for (long i = 0; i < ep - sp + 1; i++) {
            long saVal = lookupSA(sp + i);
            int recordId = searchOffset((int) saVal);
            if(!recordIds.contains(recordId)) {
                results.add(getRecord(recordId));
                recordIds.add(recordId);
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
    @Override
    public byte[][] recordRangeSearch(byte[] queryBegin, byte[] queryEnd) {
        Set<Integer> recordIds = new HashSet<Integer>();
        ArrayList<byte[]> results = new ArrayList<byte[]>();
        Range rangeBegin = getRange(queryBegin);
        Range rangeEnd = getRange(queryEnd);

        long sp = rangeBegin.first, ep = rangeEnd.second;
        if (ep - sp + 1 <= 0) {
            return new byte[0][0];
        }

        for (long i = 0; i < ep - sp + 1; i++) {
            long saVal = lookupSA(sp + i);
            int recordId = searchOffset((int) saVal);
            if(!recordIds.contains(recordId)) {
                results.add(getRecord(recordId));
                recordIds.add(recordId);
            }
        }

        return results.toArray(new byte[results.size()][]);
    }

    /**
     * Perform multiple searches with different query types and return the intersection of the results.
     *
     * @param queryTypes The QueryType corresponding to each query
     * @param queries The actual query parameters associated with each query
     * @return The records matching the multi-search queries.
     */
    @Override
    public byte[][] multiSearch(QueryType[] queryTypes, byte[][][] queries) {
        assert(queryTypes.length == queries.length);
        Set<Integer> recordIds = new HashSet<Integer>();
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
        Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
        {
            long sp = firstRange.first, ep = firstRange.second;
            for (long i = 0; i < ep - sp + 1; i++) {
                long saVal = lookupSA(sp + i);
                int recordId = searchOffset((int) saVal);
                recordIds.add(recordId);
                counts.put(recordId, 1);
            }
        }

        ranges.remove(firstRange);
        for (Range range: ranges) {
            long sp = range.first, ep = range.second;

            for (long i = 0; i < ep - sp + 1; i++) {
                long saVal = lookupSA(sp + i);
                int recordId = searchOffset((int) saVal);
                if (recordIds.contains(recordId)) {
                    counts.put(recordId, counts.get(recordId) + 1);
                }
            }
        }

        for (int recordId: recordIds) {
            if (counts.get(recordId) == numRanges) {
                results.add(getRecord(recordId));
            }
        }

        return results.toArray(new byte[results.size()][]);
    }

    /**
     * Search for all records that contain a particular regular expression.
     *
     * @param query The regular expression (UTF-8 encoded).
     * @return The records that contain the regular search expression.
     * @throws RegExParsingException
     */
    @Override
    public byte[][] recordSearchRegex(String query) throws RegExParsingException {
        Map<Long, Integer> regexOffsetResults = regexSearch(query);
        Set<Integer> recordIds = new HashSet<Integer>();
        ArrayList<byte[]> results = new ArrayList<byte[]>();
        for(Long offset: regexOffsetResults.keySet()) {
            int recordId = searchOffset(offset.intValue());
            if(!recordIds.contains(recordId)) {
                results.add(getRecord(recordId));
                recordIds.add(recordId);
            }
        }
        return results.toArray(new byte[results.size()][]);
    }

    /**
     * Extract a part of all records.
     *
     * @param offset Offset into record.
     * @param length Length of part to be extracted.
     * @return Extracted data.
     */
    @Override
    public byte[][] extractRecords(int offset, int length) {
        byte[][] records = new byte[offsets.length][];
        for(int i = 0; i < records.length; i++) {
            int curOffset = offsets[i] + offset;
            int nextOffset = (i == records.length - 1) ? getOriginalSize() : offsets[i + 1];
            length = (length < nextOffset - curOffset - 1) ? length : nextOffset - curOffset - 1;
            records[i] = extract(curOffset, length);
        }
        return records;
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
    @Override
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
    @Override
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
    @Override
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
    @Override
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
    @Override
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
    @Override
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
    @Override
    public void memoryMap(String path) throws IOException {
        File file = new File(path);
        long size = file.length();
        FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();

        ByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
        mapFromBuffer(buf);
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
