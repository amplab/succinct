package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.dictionary.Tables;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.*;

public class SuccinctIndexedBuffer extends SuccinctBuffer {

    private static final long serialVersionUID = -8357331195541317163L;

    protected static byte RECORD_DELIM = '\n';
    protected long[] offsets;

    /**
     * Constructor to initialize SuccinctIndexedBuffer from input byte array, offsets corresponding to records, and
     * context length.
     *
     * @param input The input byte array.
     * @param offsets Offsets corresponding to records.
     * @param contextLen Context Length.
     */
    public SuccinctIndexedBuffer(byte[] input, long[] offsets, int contextLen) {
        super(input, contextLen);
        this.offsets = offsets;
    }

    /**
     * Constructor to initialize SuccinctIndexedBuffer from input byte array and offsets corresponding to records
     *
     * @param input The input byte array.
     * @param offsets Offsets corresponding to records.
     */
    public SuccinctIndexedBuffer(byte[] input, long[] offsets) {
        this(input, offsets, 3);
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
        return extract((int)offsets[i], length);
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
    public Long[] recordSearchOffsets(byte[] query) {
        Set<Long> results = new HashSet<Long>();
        Range<Long, Long> range;
        range = getRange(query);

        long sp = range.first, ep = range.second;
        if (ep - sp + 1 <= 0) {
            return new Long[0];
        }

        for (long i = 0; i < ep - sp + 1; i++) {
            results.add(offsets[searchOffset(lookupSA(sp + i))]);
        }

        return results.toArray(new Long[results.size()]);
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
        Range<Long, Long> range;
        range = getRange(query);

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
        Range<Long, Long> rangeBegin, rangeEnd;
        rangeBegin = getRange(queryBegin);
        rangeEnd = getRange(queryEnd);

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
    private ArrayList<Range<Long, Long>> unionRanges(ArrayList<Range<Long, Long>> ranges) {
        if(ranges.size() <= 1)
            return ranges;

        Stack<Range<Long, Long>> rangeStack = new Stack<Range<Long, Long>>();

        Collections.sort(ranges);
        rangeStack.push(ranges.get(0));

        for(int i = 0; i < ranges.size(); i++) {
            Range<Long, Long> top = rangeStack.peek();
            if(top.second < ranges.get(i).first) {
                rangeStack.push(ranges.get(i));
            } else if(top.second < ranges.get(i).second) {
                top.second = ranges.get(i).second;
                rangeStack.pop();
                rangeStack.push(top);
            }
        }

        return new ArrayList<Range<Long, Long>>(rangeStack);
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
        ArrayList<Range<Long, Long>> ranges = new ArrayList<Range<Long, Long>>();
        for (int qid = 0; qid < queries.length; qid++) {
            Range<Long, Long> range = getRange(queries[qid]);
            if (range.second - range.first + 1 > 0) {
                ranges.add(range);
            }
        }

        // Union of all ranges
        ranges = unionRanges(ranges);

        for (Range<Long, Long> range : ranges) {
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

    class RangeSizeComparator implements Comparator<Range<Long, Long>> {
        @Override
        public int compare(Range<Long, Long> r1, Range<Long, Long> r2) {
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
        ArrayList<Range<Long, Long>> ranges = new ArrayList<Range<Long, Long>>();
        for (int qid = 0; qid < queries.length; qid++) {
            Range<Long, Long> range = getRange(queries[qid]);
            if (range.second - range.first + 1 > 0) {
                ranges.add(range);
            } else {
                return new byte[0][0];
            }
        }

        Collections.sort(ranges, new RangeSizeComparator());

        // Populate the set of offsets corresponding to the first range
        Range<Long, Long> firstRange = ranges.get(0);
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
        for(Range<Long, Long> range: ranges) {
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
        ArrayList<Range<Long, Long>> ranges = new ArrayList<Range<Long, Long>>();
        for (int qid = 0; qid < queries.length; qid++) {
            Range<Long, Long> range;

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
                    Range<Long, Long> rangeBegin, rangeEnd;
                    rangeBegin = getRange(queryBegin);
                    rangeEnd = getRange(queryEnd);
                    range = new Range<Long, Long>(rangeBegin.first, rangeEnd.second);
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
        Range<Long, Long> firstRange = ranges.get(0);
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
        for (Range<Long, Long> range: ranges) {
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
     * Serialize SuccinctIndexedBuffer to OutputStream.
     *
     * @param oos ObjectOutputStream to write to.
     * @throws IOException
     */
    private void writeObject(ObjectOutputStream oos) throws IOException {
        resetBuffers();

        WritableByteChannel dataChannel = Channels.newChannel(oos);

        oos.writeObject(offsets);

        dataChannel.write(metadata.order(ByteOrder.nativeOrder()));

        oos.writeInt(this.contextLen);

        dataChannel.write(alphabetmap.order(ByteOrder.nativeOrder()));

        ByteBuffer bufContext = ByteBuffer.allocate(contextmap.capacity() * 8);
        bufContext.asLongBuffer().put(contextmap);
        dataChannel.write(bufContext.order(ByteOrder.nativeOrder()));

        dataChannel.write(alphabet.order(ByteOrder.nativeOrder()));

        ByteBuffer bufSA = ByteBuffer.allocate(sa.capacity() * 8);
        bufSA.asLongBuffer().put(sa);
        dataChannel.write(bufSA.order(ByteOrder.nativeOrder()));

        ByteBuffer bufISA = ByteBuffer.allocate(isa.capacity() * 8);
        bufISA.asLongBuffer().put(isa);
        dataChannel.write(bufISA.order(ByteOrder.nativeOrder()));

        oos.writeLong((long) neccol.capacity());
        ByteBuffer bufNecCol = ByteBuffer.allocate(neccol.capacity() * 8);
        bufNecCol.asLongBuffer().put(neccol);
        dataChannel.write(bufNecCol.order(ByteOrder.nativeOrder()));

        oos.writeLong((long) necrow.capacity());
        ByteBuffer bufNecRow = ByteBuffer.allocate(necrow.capacity() * 8);
        bufNecRow.asLongBuffer().put(necrow);
        dataChannel.write(bufNecRow.order(ByteOrder.nativeOrder()));

        oos.writeLong((long) rowoffsets.capacity());
        ByteBuffer bufRowOff = ByteBuffer.allocate(rowoffsets.capacity() * 8);
        bufRowOff.asLongBuffer().put(rowoffsets);
        dataChannel.write(bufRowOff.order(ByteOrder.nativeOrder()));

        oos.writeLong((long) coloffsets.capacity());
        ByteBuffer bufColOff = ByteBuffer.allocate(coloffsets.capacity() * 8);
        bufColOff.asLongBuffer().put(coloffsets);
        dataChannel.write(bufColOff.order(ByteOrder.nativeOrder()));

        oos.writeLong((long) celloffsets.capacity());
        ByteBuffer bufCellOff = ByteBuffer.allocate(celloffsets.capacity() * 8);
        bufCellOff.asLongBuffer().put(celloffsets);
        dataChannel.write(bufCellOff.order(ByteOrder.nativeOrder()));

        oos.writeLong((long) rowsizes.capacity());
        ByteBuffer bufRowSizes = ByteBuffer.allocate(rowsizes.capacity() * 4);
        bufRowSizes.asIntBuffer().put(rowsizes);
        dataChannel.write(bufRowSizes.order(ByteOrder.nativeOrder()));

        oos.writeLong((long) colsizes.capacity());
        ByteBuffer bufColSizes = ByteBuffer.allocate(colsizes.capacity() * 4);
        bufColSizes.asIntBuffer().put(colsizes);
        dataChannel.write(bufColSizes.order(ByteOrder.nativeOrder()));

        oos.writeLong((long) roff.capacity());
        ByteBuffer bufROff = ByteBuffer.allocate(roff.capacity() * 4);
        bufROff.asIntBuffer().put(roff);
        dataChannel.write(bufROff.order(ByteOrder.nativeOrder()));

        oos.writeLong((long) coff.capacity());
        ByteBuffer bufCoff = ByteBuffer.allocate(coff.capacity() * 4);
        bufCoff.asIntBuffer().put(coff);
        dataChannel.write(bufCoff.order(ByteOrder.nativeOrder()));

        for (int i = 0; i < wavelettree.length; i++) {
            long wavelettreeSize = (long) ((wavelettree[i] == null) ? 0
                    : wavelettree[i].capacity());
            oos.writeLong(wavelettreeSize);
            if (wavelettreeSize != 0) {
                dataChannel
                        .write(wavelettree[i].order(ByteOrder.nativeOrder()));
            }
        }

        resetBuffers();
    }

    /**
     * Deserialize SuccinctIndexedBuffer from InputStream.
     *
     * @param ois ObjectInputStream to read from.
     * @throws IOException
     */
    private void readObject(ObjectInputStream ois)
            throws ClassNotFoundException, IOException {
        Tables.init();

        ReadableByteChannel dataChannel = Channels.newChannel(ois);

        offsets = (long [])ois.readObject();

        this.setOriginalSize((int) ois.readLong());
        this.sampledSASize = (int) ois.readLong();
        this.alphaSize = ois.readInt();
        this.sigmaSize = ois.readInt();
        this.bits = ois.readInt();
        this.sampledSABits = ois.readInt();
        this.samplingBase = ois.readInt();
        this.samplingRate = ois.readInt();
        this.numContexts = ois.readInt();

        metadata = ByteBuffer.allocate(44);
        metadata.putLong(getOriginalSize());
        metadata.putLong(sampledSASize);
        metadata.putInt(alphaSize);
        metadata.putInt(sigmaSize);
        metadata.putInt(bits);
        metadata.putInt(sampledSABits);
        metadata.putInt(samplingBase);
        metadata.putInt(samplingRate);
        metadata.putInt(numContexts);
        metadata.position(0);

        this.contextLen = ois.readInt();

        int cmapSize = this.alphaSize;
        this.alphabetmap = ByteBuffer.allocate(cmapSize * (1 + 8 + 4));
        dataChannel.read(this.alphabetmap);
        this.alphabetmap.position(0);

        // Deserialize cmap
        alphabetMap = new HashMap<Byte, Pair<Long, Integer>>();
        for (int i = 0; i < this.alphaSize; i++) {
            byte c = alphabetmap.get();
            long v1 = alphabetmap.getLong();
            int v2 = alphabetmap.getInt();
            alphabetMap.put(c, new Pair<Long, Integer>(v1, v2));
        }

        // Read contexts
        int contextsSize = this.numContexts;
        ByteBuffer contextBuf = ByteBuffer.allocate(contextsSize * 8 * 2);
        dataChannel.read(contextBuf);
        contextBuf.position(0);
        this.contextmap = contextBuf.asLongBuffer();

        // Deserialize contexts
        contextMap = new HashMap<Long, Long>();
        for (int i = 0; i < this.numContexts; i++) {
            long v1 = contextmap.get();
            long v2 = contextmap.get();
            contextMap.put(v1, v2);
        }

        // Read slist
        int slistSize = this.alphaSize;
        this.alphabet = ByteBuffer.allocate(slistSize);
        dataChannel.read(this.alphabet);
        this.alphabet.position(0);

        // Read sa
        int saSize = (sampledSASize * sampledSABits) / 64 + 1;
        ByteBuffer saBuf = ByteBuffer.allocate(saSize * 8);
        dataChannel.read(saBuf);
        saBuf.position(0);
        this.sa = saBuf.asLongBuffer();

        // Read sainv
        int isaSize = (sampledSASize * sampledSABits) / 64 + 1;
        ByteBuffer isaBuf = ByteBuffer.allocate(isaSize * 8);
        dataChannel.read(isaBuf);
        isaBuf.position(0);
        this.isa = isaBuf.asLongBuffer();

        // Read neccol
        int neccolSize = (int) ois.readLong();
        ByteBuffer neccolBuf = ByteBuffer.allocate(neccolSize * 8);
        dataChannel.read(neccolBuf);
        neccolBuf.position(0);
        this.neccol = neccolBuf.asLongBuffer();

        // Read necrow
        int necrowSize = (int) ois.readLong();
        ByteBuffer necrowBuf = ByteBuffer.allocate(necrowSize * 8);
        dataChannel.read(necrowBuf);
        necrowBuf.position(0);
        this.necrow = necrowBuf.asLongBuffer();

        // Read rowoffsets
        int rowoffsetsSize = (int) ois.readLong();
        ByteBuffer rowoffsetsBuf = ByteBuffer.allocate(rowoffsetsSize * 8);
        dataChannel.read(rowoffsetsBuf);
        rowoffsetsBuf.position(0);
        this.rowoffsets = rowoffsetsBuf.asLongBuffer();

        // Read coloffsets
        int coloffsetsSize = (int) ois.readLong();
        ByteBuffer coloffsetsBuf = ByteBuffer.allocate(coloffsetsSize * 8);
        dataChannel.read(coloffsetsBuf);
        coloffsetsBuf.position(0);
        this.coloffsets = coloffsetsBuf.asLongBuffer();

        // Read celloffsets
        int celloffsetsSize = (int) ois.readLong();
        ByteBuffer celloffsetsBuf = ByteBuffer.allocate(celloffsetsSize * 8);
        dataChannel.read(celloffsetsBuf);
        celloffsetsBuf.position(0);
        this.celloffsets = celloffsetsBuf.asLongBuffer();

        // Read rowsizes
        int rowsizesSize = (int) ois.readLong();
        ByteBuffer rowsizesBuf = ByteBuffer.allocate(rowsizesSize * 4);
        dataChannel.read(rowsizesBuf);
        rowsizesBuf.position(0);
        this.rowsizes = rowsizesBuf.asIntBuffer();

        int colsizesSize = (int) ois.readLong();
        ByteBuffer colsizesBuf = ByteBuffer.allocate(colsizesSize * 4);
        dataChannel.read(colsizesBuf);
        colsizesBuf.position(0);
        this.colsizes = colsizesBuf.asIntBuffer();

        int roffSize = (int) ois.readLong();
        ByteBuffer roffBuf = ByteBuffer.allocate(roffSize * 4);
        dataChannel.read(roffBuf);
        roffBuf.position(0);
        this.roff = roffBuf.asIntBuffer();

        int coffSize = (int) ois.readLong();
        ByteBuffer coffBuf = ByteBuffer.allocate(coffSize * 4);
        dataChannel.read(coffBuf);
        coffBuf.position(0);
        this.coff = coffBuf.asIntBuffer();

        wavelettree = new ByteBuffer[contextsSize];
        for (int i = 0; i < contextsSize; i++) {
            long wavelettreeSize = ois.readLong();
            wavelettree[i] = null;
            if (wavelettreeSize != 0) {
                ByteBuffer wavelettreeBuf = ByteBuffer
                        .allocate((int) wavelettreeSize);
                dataChannel.read(wavelettreeBuf);
                wavelettree[i] = (ByteBuffer) wavelettreeBuf.position(0);
            }
        }
    }
}
