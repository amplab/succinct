package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.bitmap.BMArray;
import edu.berkeley.cs.succinct.bitmap.BitMap;
import edu.berkeley.cs.succinct.dictionary.Tables;
import edu.berkeley.cs.succinct.qsufsort.QSufSort;
import edu.berkeley.cs.succinct.util.CommonUtils;
import edu.berkeley.cs.succinct.util.SerializedOperations;
import edu.berkeley.cs.succinct.util.buffers.ThreadSafeByteBuffer;
import edu.berkeley.cs.succinct.util.buffers.ThreadSafeIntBuffer;
import edu.berkeley.cs.succinct.util.buffers.ThreadSafeLongBuffer;
import edu.berkeley.cs.succinct.wavelettree.WaveletTree;
import org.apache.hadoop.hdfs.server.common.Storage;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.*;

public class SuccinctCore implements Serializable {

    private static final long serialVersionUID = 1382615274437547247L;

    public transient static final byte EOF = 1;

    protected transient ThreadSafeByteBuffer metadata;
    protected transient ThreadSafeByteBuffer alphabetmap;
    protected transient ThreadSafeLongBuffer contextmap;
    protected transient ThreadSafeByteBuffer alphabet;
    protected transient ThreadSafeLongBuffer sa;
    protected transient ThreadSafeLongBuffer isa;
    protected transient ThreadSafeLongBuffer neccol;
    protected transient ThreadSafeLongBuffer necrow;
    protected transient ThreadSafeLongBuffer rowoffsets;
    protected transient ThreadSafeLongBuffer coloffsets;
    protected transient ThreadSafeLongBuffer celloffsets;
    protected transient ThreadSafeIntBuffer rowsizes;
    protected transient ThreadSafeIntBuffer colsizes;
    protected transient ThreadSafeIntBuffer roff;
    protected transient ThreadSafeIntBuffer coff;
    protected transient ThreadSafeByteBuffer[] wavelettree;

    // Metadata
    private transient int originalSize;
    private transient int sampledSASize;
    private transient int alphaSize;
    private transient int sigmaSize;
    private transient int bits;
    private transient int sampledSABits;
    private transient int samplingBase;
    private transient int samplingRate;
    private transient int numContexts;
    private transient int contextLen;

    public enum StorageMode {
        MEMORY,
        MEMORY_MAPPED
    }

    protected transient StorageMode storageMode;

    protected transient HashMap<Byte, Pair<Long, Integer>> alphabetMap;
    protected transient Map<Long, Long> contextMap;

    /**
     *
     * Constructor to initialize SuccinctCore from input byte array and specified context length.
     * @param input Input byte array.
     * @param contextLen Context length.
     */
    public SuccinctCore(byte[] input, int contextLen) {

        this.setContextLen(contextLen);

        // Initializing Table data
        Tables.init();

        // Append the EOF byte
        int end = input.length;
        input = Arrays.copyOf(input, input.length + 1);
        input[end] = EOF;

        // Construct Succinct data-structures
        construct(input);
    }

    public SuccinctCore(String path, StorageMode storageMode) {
        this.storageMode = storageMode;
        try {
            Tables.init();
            if(storageMode == StorageMode.MEMORY) {
                readFromFile(path);
            } else {
                memoryMap(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the original size.
     *
     * @return The originalSize.
     */
    public int getOriginalSize() {
        return originalSize;
    }

    /**
     * Set the original size.
     *
     * @param originalSize The originalSize to set.
     */
    public void setOriginalSize(int originalSize) {
        this.originalSize = originalSize;
    }

    public int getSampledSASize() {
        return sampledSASize;
    }

    public void setSampledSASize(int sampledSASize) {
        this.sampledSASize = sampledSASize;
    }

    public int getAlphaSize() {
        return alphaSize;
    }

    public void setAlphaSize(int alphaSize) {
        this.alphaSize = alphaSize;
    }

    public int getSigmaSize() {
        return sigmaSize;
    }

    public void setSigmaSize(int sigmaSize) {
        this.sigmaSize = sigmaSize;
    }

    public int getBits() {
        return bits;
    }

    public void setBits(int bits) {
        this.bits = bits;
    }

    public int getSampledSABits() {
        return sampledSABits;
    }

    public void setSampledSABits(int sampledSABits) {
        this.sampledSABits = sampledSABits;
    }

    public int getSamplingBase() {
        return samplingBase;
    }

    public void setSamplingBase(int samplingBase) {
        this.samplingBase = samplingBase;
    }

    public int getSamplingRate() {
        return samplingRate;
    }

    public void setSamplingRate(int samplingRate) {
        this.samplingRate = samplingRate;
    }

    public int getNumContexts() {
        return numContexts;
    }

    public void setNumContexts(int numContexts) {
        this.numContexts = numContexts;
    }

    public int getContextLen() {
        return contextLen;
    }

    public void setContextLen(int contextLen) {
        this.contextLen = contextLen;
    }

    public static class Pair<T1, T2> {

        public T1 first;
        public T2 second;

        /**
         * Constructor to initialize pair
         *
         * @param first First element.
         * @param second Second element.
         */
        public Pair(T1 first, T2 second) {
            this.first = first;
            this.second = second;
        }

        /**
         * Overrides toString() for Object.
         * @return String representation of the pair.
         */
        @Override
        public String toString() {
            return "(" + first.toString() + ", " + second.toString() + ")";
        }
    }

    /**
     * Represents a numeric range, [first, second] (inclusive). It is an
     * invalid/empty range iff second < first.
     */
    class Range implements Comparable<Range> {
        public long first, second;

        /**
         * Constructor to initialize pair
         *
         * @param first  First element.
         * @param second Second element.
         */
        public Range(long first, long second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public int compareTo(Range that) {
            long diff1 = this.first - that.first;
            long diff2 = this.second - that.second;
            if (diff1 == 0) {
                return diff2 < 0 ? -1 : (diff2 == 0 ? 0 : 1);
            } else {
                return diff1 < 0 ? -1 : 1;
            }
        }

        @Override
        public String toString() {
            return String.format("[%d, %d]", first, second);
        }
    }

    /**
     * Lookup NPA at specified index.
     *
     * @param i Index into NPA.
     * @return Value of NPA at specified index.
     */
    public long lookupNPA(long i) {
        if(i > getOriginalSize() - 1 || i < 0) {
            throw new ArrayIndexOutOfBoundsException("NPA index out of bounds: i = "
                    + i + " originalSize = " + getOriginalSize());
        }
        int colId, rowId, cellId, cellOff, contextSize, contextPos;
        long colOff, rowOff;

        // Search columnoffset
        colId = SerializedOperations.ArrayOps.getRank1(coloffsets.buffer(), 0, getSigmaSize(), i) - 1;

        // Get columnoffset
        colOff = coloffsets.get(colId);

        // Search celloffsets
        cellId = SerializedOperations.ArrayOps.getRank1(celloffsets.buffer(), coff.get(colId),
                colsizes.get(colId), i - colOff) - 1;

        // Get position within cell
        cellOff = (int) (i - colOff - celloffsets.get(coff.get(colId) + cellId));

        // Search rowoffsets
        rowId = (int) neccol.get(coff.get(colId) + cellId);

        // Get rowoffset
        rowOff = rowoffsets.get(rowId);

        // Get context size
        contextSize = rowsizes.get(rowId);

        // Get context position
        contextPos = SerializedOperations.ArrayOps.getRank1(necrow.buffer(), roff.get(rowId),
                rowsizes.get(rowId), colId) - 1;

        long cellValue = cellOff;

        if (wavelettree[rowId] != null) {
            cellValue = SerializedOperations.WaveletTreeOps.getValue(
                    (ByteBuffer) wavelettree[rowId].rewind(), contextPos,
                    cellOff, 0, contextSize - 1);
            wavelettree[rowId].rewind();
        }

        return rowOff + cellValue;
    }

    /**
     * Lookup SA at specified index.
     *
     * @param i Index into SA.
     * @return Value of SA at specified index.
     */
    public long lookupSA(long i) {

        if(i > getOriginalSize() - 1 || i < 0) {
            throw new ArrayIndexOutOfBoundsException("SA index out of bounds: i = "
                    + i + " originalSize = " + getOriginalSize());
        }

        long numHops = 0;
        while (i % getSamplingRate() != 0) {
            i = lookupNPA(i);
            numHops++;
        }
        long sampledValue = SerializedOperations.BMArrayOps.getVal(sa.buffer(), (int) (i / getSamplingRate()),
                getSampledSABits());

        if(sampledValue < numHops) {
            return getOriginalSize() - (numHops - sampledValue);
        }

        return sampledValue - numHops;
    }

    /**
     * Lookup ISA at specified index.
     *
     * @param i Index into ISA.
     * @return Value of ISA at specified index.
     */
    public long lookupISA(long i) {

        if(i > getOriginalSize() - 1 || i < 0) {
            throw new ArrayIndexOutOfBoundsException("ISA index out of bounds: i = "
                    + i + " originalSize = " + getOriginalSize());
        }

        int sampleIdx = (int) (i / getSamplingRate());
        long pos = SerializedOperations.BMArrayOps.getVal(isa.buffer(), sampleIdx, getSampledSABits());
        i -= (sampleIdx * getSamplingRate());
        while (i != 0) {
            pos = lookupNPA(pos);
            i--;
        }
        return pos;
    }

    /**
     * Construct Succinct data structures from input byte array.
     *
     * @param input Input byte array.
     */
    private void construct(byte[] input) {

        // Collect metadata
        setOriginalSize(input.length);
        setBits(CommonUtils.intLog2(getOriginalSize() + 1));
        setSamplingBase(5); // Hard coded
        setSamplingRate((1 << getSamplingBase()));

        // Construct SA, ISA
        QSufSort qsuf = new QSufSort();
        qsuf.buildSuffixArray(input);

        BMArray cSA = new BMArray(qsuf.getSA(), 0, getOriginalSize());
        BMArray cISA = new BMArray(qsuf.getISA(), 0, getOriginalSize());

        this.constructAux(cSA, input);

        setSigmaSize(0); // TODO: Combine sigmaSize and alphaSize
        int alphaBits = CommonUtils.intLog2(getAlphaSize() + 1);
        BMArray textBitMap = new BMArray(getOriginalSize(), alphaBits);
        for (int i = 0; i < getOriginalSize(); i++) {
            textBitMap.setVal(i, alphabetMap.get(input[i]).second);
            setSigmaSize(Math.max(alphabetMap.get(input[i]).second, getSigmaSize()));
        }
        setSigmaSize(getSigmaSize() + 1);

        this.constructNPA(textBitMap, cSA, cISA);
        this.constructCSA(cSA);

        setNumContexts(contextMap.size());
        metadata = ThreadSafeByteBuffer.allocate(36);
        metadata.putInt(getOriginalSize());
        metadata.putInt(getSampledSASize());
        metadata.putInt(getAlphaSize());
        metadata.putInt(getSigmaSize());
        metadata.putInt(getBits());
        metadata.putInt(getSampledSABits());
        metadata.putInt(getSamplingBase());
        metadata.putInt(getSamplingRate());
        metadata.putInt(getNumContexts());
        metadata.rewind();

    }

    /**
     * Construct auxiliary data structures.
     *
     * @param SA Suffix Array.
     * @param input Input byte array.
     */
    private void constructAux(BMArray SA, byte[] input) {

        ArrayList<Long> CinvIndex = new ArrayList<Long>();
        byte[] alphabetArray;
        alphabetMap = new HashMap<Byte, Pair<Long, Integer>>();
        setAlphaSize(1);

        for (long i = 1; i < input.length; ++i) {
            if (input[(int) SA.getVal((int) i)] != input[(int) SA.getVal((int) (i - 1))]) {
                CinvIndex.add(i);
                setAlphaSize(getAlphaSize() + 1);
            }
        }
        setAlphaSize(getAlphaSize() + 1);

        alphabetArray = new byte[getAlphaSize()];

        alphabetArray[0] = input[(int) SA.getVal(0)];
        alphabetMap.put(alphabetArray[0], new Pair<Long, Integer>(0L, 0));
        long i;
        for (i = 1; i < getAlphaSize() - 1; i++) {
            long sel = CinvIndex.get((int) i - 1);
            alphabetArray[(int) i] = input[(int) SA.getVal((int) sel)];
            alphabetMap.put(alphabetArray[(int) i], new Pair<Long, Integer>(sel, (int) i));
        }
        alphabetMap.put((byte) 0, new Pair<Long, Integer>((long) input.length, (int) i));
        alphabetArray[(int) i] = (char) 0;

        // Serialize cmap
        alphabetmap = ThreadSafeByteBuffer.allocate(alphabetMap.size() * (1 + 8 + 4));
        for (Byte c : alphabetMap.keySet()) {
            Pair<Long, Integer> cval = alphabetMap.get(c);
            alphabetmap.put(c);
            alphabetmap.putLong(cval.first);
            alphabetmap.putInt(cval.second);
        }
        alphabetmap.rewind();

        // Serialize S
        alphabet = ThreadSafeByteBuffer.allocate(alphabetArray.length);
        for (int j = 0; j < alphabetArray.length; j++) {
            alphabet.put(alphabetArray[j]);
        }
        alphabet.rewind();

    }

    /**
     * Constructs NPA.
     *
     * @param T Input byte array as a bitmap.
     * @param SA Suffix Array.
     * @param ISA Inverse Suffix Array.
     */
    private void constructNPA(BMArray T, BMArray SA, BMArray ISA) {

        ArrayList<Long> rowOffsets;
        ArrayList<Long> colOffsets;
        ArrayList<ArrayList<Long>> cellOffsets;
        ArrayList<ArrayList<Long>> necCol;
        ArrayList<ArrayList<Long>> necRow;

        int necColSize = 0, necRowSize = 0, cellOffsetsSize = 0;

        int k = 0, contextVal, contextId;
        long k1, k2, lOff = 0, npaVal, p = 0;
        HashMap<Long, Long> contextSizesMap = new HashMap<Long, Long>();

        ArrayList<ArrayList<ArrayList<Long>>> table;
        ArrayList<Pair<ArrayList<Long>, Character>> context = new ArrayList<Pair<ArrayList<Long>, Character>>();
        ArrayList<Long> cell;
        ArrayList<Long> contextValues = new ArrayList<Long>();
        ArrayList<Long> contextColumnIds = new ArrayList<Long>();
        boolean flag;
        long[] sizes, starts, c_sizes;
        long last_i = 0;

        contextMap = new TreeMap<Long, Long>();
        for (long i = 0; i < getOriginalSize(); i++) {
            long contextValue = getContextVal(T, i);
            contextMap.put(contextValue, 0L);
            if (!contextSizesMap.containsKey(contextValue)) {
                contextSizesMap.put(contextValue, 1L);
            } else {
                contextSizesMap.put(contextValue,
                        contextSizesMap.get(contextValue) + 1);
            }
        }

        sizes = new long[contextMap.size()];
        c_sizes = new long[contextMap.size()];
        starts = new long[contextMap.size()];

        for (Map.Entry<Long, Long> currentContext : contextMap.entrySet()) {
            sizes[k] = contextSizesMap.get(currentContext.getKey());
            starts[k] = getOriginalSize();
            contextMap.put(currentContext.getKey(), (long) k);
            k++;
        }

        assert (k == contextMap.size());
        contextSizesMap.clear();

        BitMap NonNullBitMap = new BitMap(k * getSigmaSize());
        table = new ArrayList<ArrayList<ArrayList<Long>>>(k);
        cellOffsets = new ArrayList<ArrayList<Long>>();
        necCol = new ArrayList<ArrayList<Long>>();
        colOffsets = new ArrayList<Long>();
        rowOffsets = new ArrayList<Long>();
        necRow = new ArrayList<ArrayList<Long>>();
        wavelettree = new ThreadSafeByteBuffer[k];

        for (int i = 0; i < k; i++) {
            table.add(new ArrayList<ArrayList<Long>>(getSigmaSize()));
            for (int j = 0; j < getSigmaSize(); j++) {
                ArrayList<Long> tableCell = new ArrayList<Long>();
                table.get(i).add(tableCell);
            }
            necRow.add(new ArrayList<Long>());
        }

        for (int i = 0; i < getSigmaSize(); i++) {
            necCol.add(new ArrayList<Long>());
            cellOffsets.add(new ArrayList<Long>());
        }

        k1 = SA.getVal(0);
        contextId = (int) getContextVal(T, (k1 + 1) % getOriginalSize());
        contextVal = contextMap.get((long) contextId).intValue();
        npaVal = ISA.getVal((int) ((SA.getVal(0) + 1) % getOriginalSize()));
        table.get(contextVal).get((int) (lOff / k)).add(npaVal);
        starts[contextVal] = Math.min(starts[contextVal], npaVal);
        c_sizes[contextVal]++;

        NonNullBitMap.setBit((int) contextVal);
        necCol.get(0).add((long) contextVal);
        necColSize++;
        colOffsets.add(0L);
        cellOffsets.get(0).add(0L);
        cellOffsetsSize++;

        for (long i = 1; i < getOriginalSize(); i++) {
            k1 = SA.getVal((int) i);
            k2 = SA.getVal((int) i - 1);

            contextId = (int) getContextVal(T, (k1 + 1) % getOriginalSize());
            contextVal = contextMap.get((long) contextId).intValue();

            // context value cannot exceed k, the number of contexts
            assert (contextVal < k);

            // If the column has changed, mark in colOffsets, cellOffsets;
            // update l offset
            if (!compareT(T, k1, k2, 1)) {
                colOffsets.add(i);
                lOff += k;
                last_i = i;
                cellOffsets.get((int) (lOff / k)).add(i - last_i);
                cellOffsetsSize++;
            } else if (!compareT(T, (k1 + 1) % getOriginalSize(), (k2 + 1) % getOriginalSize(), getContextLen())) {
                // Context has changed; mark in cellOffsets
                cellOffsets.get((int) (lOff / k)).add(i - last_i);
                cellOffsetsSize++;
            }

            // If we haven't marked it already, mark current cell as non empty
            if (NonNullBitMap.getBit((int) (lOff + contextVal)) == 0) {
                NonNullBitMap.setBit((int) (lOff + contextVal));
                necCol.get((int) (lOff / k)).add((long) contextVal);
                necColSize++;
            }

            // Obtain actual npa value
            npaVal = ISA.getVal((int) ((SA.getVal((int) i) + 1) % getOriginalSize()));

            assert (lOff / k < getSigmaSize());
            // Push npa value to npa table: note indices. indexed by context
            // value, and lOffs / k
            table.get(contextVal).get((int) (lOff / k)).add(npaVal);
            starts[contextVal] = Math.min(starts[contextVal], npaVal);
            if (table.get(contextVal).get((int) (lOff / k)).size() == 1) {
                c_sizes[contextVal]++;
            }
        }

        for (int i = 0; i < k; i++) {
            flag = false;
            for (int j = 0; j < getSigmaSize(); j++) {
                if (!flag && NonNullBitMap.getBit(i + j * k) == 1) {
                    rowOffsets.add(p);
                    p += table.get(i).get(j).size();
                    flag = true;
                } else if (NonNullBitMap.getBit(i + j * k) == 1) {
                    p += table.get(i).get(j).size();
                }

                if (NonNullBitMap.getBit(i + j * k) == 1) {
                    cell = new ArrayList<Long>();
                    for (long t = 0; t < table.get(i).get(j).size(); t++) {
                        cell.add(table.get(i).get(j).get((int) t)
                                - starts[i]);
                    }
                    assert (cell.size() > 0);
                    context.add(new Pair<ArrayList<Long>, Character>(cell, (char) j));

                    necRow.get(i).add((long) j);
                    necRowSize++;
                }
            }

            assert (context.size() > 0);
            for (int j = 0; j < context.size(); j++) {
                assert (context.get(j).first.size() > 0);
                for (int t = 0; t < context.get(j).first.size(); t++) {
                    contextValues.add(context.get(j).first.get(t));
                    contextColumnIds.add((long) j);
                }
            }

            assert (contextValues.size() > 0);
            assert (contextColumnIds.size() == contextValues.size());

            WaveletTree wTree = new WaveletTree(0, context.size() - 1,
                    contextValues, contextColumnIds);
            wavelettree[i] = ThreadSafeByteBuffer.fromByteBuffer(wTree.getByteBuffer());

            contextValues.clear();
            contextColumnIds.clear();
            context.clear();
        }
        table.clear();

        // Serialize neccol
        neccol = ThreadSafeLongBuffer.allocate(necColSize);
        for (int i = 0; i < necCol.size(); i++) {
            for (int j = 0; j < necCol.get(i).size(); j++) {
                neccol.put(necCol.get(i).get(j));
            }
        }

        neccol.rewind();

        // Serialize necrow
        necrow = ThreadSafeLongBuffer.allocate(necRowSize);
        for (int i = 0; i < necRow.size(); i++) {
            for (int j = 0; j < necRow.get(i).size(); j++) {
                necrow.put(necRow.get(i).get(j));
            }
        }
        necrow.rewind();

        // Serialize rowoffsets
        rowoffsets = ThreadSafeLongBuffer.allocate(rowOffsets.size());
        for (int i = 0; i < rowOffsets.size(); i++) {
            rowoffsets.put(rowOffsets.get(i));
        }
        rowoffsets.rewind();

        // Serialize coloffsets
        coloffsets = ThreadSafeLongBuffer.allocate(colOffsets.size());
        for (int i = 0; i < colOffsets.size(); i++) {
            coloffsets.put(colOffsets.get(i));
        }
        coloffsets.rewind();

        // Serialize celloffsets
        celloffsets = ThreadSafeLongBuffer.allocate(cellOffsetsSize);
        for (int i = 0; i < cellOffsets.size(); i++) {
            for (int j = 0; j < cellOffsets.get(i).size(); j++) {
                celloffsets.put(cellOffsets.get(i).get(j));
            }
        }
        celloffsets.rewind();

        // Serialize rowsizes
        rowsizes = ThreadSafeIntBuffer.allocate(necRow.size());
        for (int i = 0; i < necRow.size(); i++) {
            rowsizes.put(necRow.get(i).size());
        }
        rowsizes.rewind();

        // Serialize colsizes
        colsizes = ThreadSafeIntBuffer.allocate(necCol.size());
        for (int i = 0; i < necCol.size(); i++) {
            colsizes.put(necCol.get(i).size());
        }
        colsizes.rewind();

        // Serialize roff
        int size = 0;
        roff = ThreadSafeIntBuffer.allocate(necRow.size());
        for (int i = 0; i < necRow.size(); i++) {
            roff.put(size);
            size += necRow.get(i).size();
        }
        roff.rewind();

        // Serialize coff
        size = 0;
        coff = ThreadSafeIntBuffer.allocate(necCol.size());
        for (int i = 0; i < necCol.size(); i++) {
            coff.put(size);
            size += necCol.get(i).size();
        }
        coff.rewind();

        // Serialize contexts
        contextmap = ThreadSafeLongBuffer.allocate(2 * contextMap.size());
        for (Long c : contextMap.keySet()) {
            contextmap.put(c);
            contextmap.put(contextMap.get(c));
        }
        contextmap.rewind();
    }

    /**
     * Compare two positions in bitmap representation of input.
     *
     * @param T Bitmap representation of input.
     * @param i First position.
     * @param j Second position.
     * @param len Length of pattern to compare.
     * @return Result of comparison.
     */
    private boolean compareT(BMArray T, long i, long j, int len) {

        for (long p = i; p < i + len; p++) {
            if (T.getVal((int) p) != T.getVal((int) j++)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Get context value at specified index in bitmap representation of input.
     *
     * @param T Bitmap representation of input.
     * @param i Index into input.
     * @return Value of context at specified index.
     */
    private long getContextVal(BMArray T, long i) {

        long val = 0;
        long max = Math.min(i + getContextLen(), getOriginalSize());
        for (long p = i; p < max; p++) {
            val = val * getSigmaSize() + T.getVal((int) p);
        }

        if (max < i + getContextLen()) {
            for (long p = 0; p < (i + getContextLen()) % getOriginalSize(); p++) {
                val = val * getSigmaSize() + T.getVal((int) p);
            }
        }

        return val;
    }

    /**
     * Construct compressed SA and ISA from bitmap representation of SA.
     *
     * @param cSA Bitmap representation of SA.
     */
    private int constructCSA(BMArray cSA) {

        setSamplingRate((1 << getSamplingBase()));
        setSampledSASize((getOriginalSize() / getSamplingRate()) + 1);
        setSampledSABits(CommonUtils.intLog2(getOriginalSize() + 1));

        long saValue;

        BMArray SA = new BMArray(getSampledSASize(), getSampledSABits());
        BMArray ISA = new BMArray(getSampledSASize(), getSampledSABits());

        for (int i = 0; i < getOriginalSize(); i++) {
            saValue = cSA.getVal(i);
            if (i % getSamplingRate() == 0) {
                SA.setVal(i / getSamplingRate(), saValue);
            }
            if (saValue % getSamplingRate() == 0) {
                ISA.setVal((int)(saValue / getSamplingRate()), i);
            }
        }

        // Serialize SA
        sa = ThreadSafeLongBuffer.wrap(SA.data);

        // Serialize ISA
        isa = ThreadSafeLongBuffer.wrap(ISA.data);

        return 0;
    }

    protected void writeToStream(DataOutputStream os) throws IOException {
        WritableByteChannel dataChannel = Channels.newChannel(os);

        dataChannel.write(metadata.order(ByteOrder.BIG_ENDIAN));

        dataChannel.write(alphabetmap.order(ByteOrder.BIG_ENDIAN));

        ByteBuffer bufContext = ByteBuffer.allocate(contextmap.limit() * 8);
        bufContext.asLongBuffer().put(contextmap.buffer());
        dataChannel.write(bufContext.order(ByteOrder.BIG_ENDIAN));

        dataChannel.write(alphabet.order(ByteOrder.BIG_ENDIAN));

        ByteBuffer bufSA = ByteBuffer.allocate(sa.limit() * 8);
        bufSA.asLongBuffer().put(sa.buffer());
        dataChannel.write(bufSA.order(ByteOrder.BIG_ENDIAN));

        ByteBuffer bufISA = ByteBuffer.allocate(isa.limit() * 8);
        bufISA.asLongBuffer().put(isa.buffer());
        dataChannel.write(bufISA.order(ByteOrder.BIG_ENDIAN));

        os.writeInt(neccol.limit());
        ByteBuffer bufNecCol = ByteBuffer.allocate(neccol.limit() * 8);
        bufNecCol.asLongBuffer().put(neccol.buffer());
        dataChannel.write(bufNecCol.order(ByteOrder.BIG_ENDIAN));

        os.writeInt(necrow.limit());
        ByteBuffer bufNecRow = ByteBuffer.allocate(necrow.limit() * 8);
        bufNecRow.asLongBuffer().put(necrow.buffer());
        dataChannel.write(bufNecRow.order(ByteOrder.BIG_ENDIAN));

        os.writeInt(rowoffsets.limit());
        ByteBuffer bufRowOff = ByteBuffer.allocate(rowoffsets.limit() * 8);
        bufRowOff.asLongBuffer().put(rowoffsets.buffer());
        dataChannel.write(bufRowOff.order(ByteOrder.BIG_ENDIAN));

        os.writeInt(coloffsets.limit());
        ByteBuffer bufColOff = ByteBuffer.allocate(coloffsets.limit() * 8);
        bufColOff.asLongBuffer().put(coloffsets.buffer());
        dataChannel.write(bufColOff.order(ByteOrder.BIG_ENDIAN));

        os.writeInt(celloffsets.limit());
        ByteBuffer bufCellOff = ByteBuffer.allocate(celloffsets.limit() * 8);
        bufCellOff.asLongBuffer().put(celloffsets.buffer());
        dataChannel.write(bufCellOff.order(ByteOrder.BIG_ENDIAN));

        os.writeInt(rowsizes.limit());
        ByteBuffer bufRowSizes = ByteBuffer.allocate(rowsizes.limit() * 4);
        bufRowSizes.asIntBuffer().put(rowsizes.buffer());
        dataChannel.write(bufRowSizes.order(ByteOrder.BIG_ENDIAN));

        os.writeInt(colsizes.limit());
        ByteBuffer bufColSizes = ByteBuffer.allocate(colsizes.limit() * 4);
        bufColSizes.asIntBuffer().put(colsizes.buffer());
        dataChannel.write(bufColSizes.order(ByteOrder.BIG_ENDIAN));

        os.writeInt(roff.limit());
        ByteBuffer bufROff = ByteBuffer.allocate(roff.limit() * 4);
        bufROff.asIntBuffer().put(roff.buffer());
        dataChannel.write(bufROff.order(ByteOrder.BIG_ENDIAN));

        os.writeInt(coff.limit());
        ByteBuffer bufCoff = ByteBuffer.allocate(coff.limit() * 4);
        bufCoff.asIntBuffer().put(coff.buffer());
        dataChannel.write(bufCoff.order(ByteOrder.BIG_ENDIAN));

        for (int i = 0; i < wavelettree.length; i++) {
            int wavelettreeSize = (wavelettree[i] == null) ? 0
                    : wavelettree[i].limit();
            os.writeInt(wavelettreeSize);
            if (wavelettreeSize != 0) {
                dataChannel.write(wavelettree[i].order(ByteOrder.BIG_ENDIAN));
            }
        }
    }


    protected void readFromStream(DataInputStream is) throws IOException {
        ReadableByteChannel dataChannel = Channels.newChannel(is);
        this.setOriginalSize(is.readInt());
        this.setSampledSASize(is.readInt());
        this.setAlphaSize(is.readInt());
        this.setSigmaSize(is.readInt());
        this.setBits(is.readInt());
        this.setSampledSABits(is.readInt());
        this.setSamplingBase(is.readInt());
        this.setSamplingRate(is.readInt());
        this.setNumContexts(is.readInt());

        metadata = ThreadSafeByteBuffer.allocate(36);
        metadata.putInt(getOriginalSize());
        metadata.putInt(getSampledSASize());
        metadata.putInt(getAlphaSize());
        metadata.putInt(getSigmaSize());
        metadata.putInt(getBits());
        metadata.putInt(getSampledSABits());
        metadata.putInt(getSamplingBase());
        metadata.putInt(getSamplingRate());
        metadata.putInt(getNumContexts());
        metadata.rewind();

        int cmapSize = this.getAlphaSize();
        this.alphabetmap = ThreadSafeByteBuffer.allocate(cmapSize * (1 + 8 + 4));
        dataChannel.read(this.alphabetmap.buffer());
        this.alphabetmap.rewind();

        // Deserialize cmap
        alphabetMap = new HashMap<Byte, Pair<Long, Integer>>();
        for (int i = 0; i < this.getAlphaSize(); i++) {
            byte c = alphabetmap.get();
            long v1 = alphabetmap.getLong();
            int v2 = alphabetmap.getInt();
            alphabetMap.put(c, new Pair<Long, Integer>(v1, v2));
        }

        // Read contexts
        int contextsSize = this.getNumContexts();
        ByteBuffer contextBuf = ByteBuffer.allocate(contextsSize * 8 * 2);
        dataChannel.read(contextBuf);
        contextBuf.rewind();
        this.contextmap = ThreadSafeLongBuffer.fromLongBuffer(contextBuf.asLongBuffer());

        // Deserialize contexts
        contextMap = new HashMap<Long, Long>();
        for (int i = 0; i < this.getNumContexts(); i++) {
            long v1 = contextmap.get();
            long v2 = contextmap.get();
            contextMap.put(v1, v2);
        }

        // Read slist
        int slistSize = this.getAlphaSize();
        this.alphabet = ThreadSafeByteBuffer.allocate(slistSize);
        dataChannel.read(this.alphabet.buffer());
        this.alphabet.rewind();

        // Read sa
        int saSize = (getSampledSASize() * getSampledSABits()) / 64 + 1;
        ByteBuffer saBuf = ByteBuffer.allocate(saSize * 8);
        dataChannel.read(saBuf);
        saBuf.rewind();
        this.sa = ThreadSafeLongBuffer.fromLongBuffer(saBuf.asLongBuffer());

        // Read sainv
        int isaSize = (getSampledSASize() * getSampledSABits()) / 64 + 1;
        ByteBuffer isaBuf = ByteBuffer.allocate(isaSize * 8);
        dataChannel.read(isaBuf);
        isaBuf.rewind();
        this.isa = ThreadSafeLongBuffer.fromLongBuffer(isaBuf.asLongBuffer());

        // Read neccol
        int neccolSize = is.readInt();
        ByteBuffer neccolBuf = ByteBuffer.allocate(neccolSize * 8);
        dataChannel.read(neccolBuf);
        neccolBuf.rewind();
        this.neccol = ThreadSafeLongBuffer.fromLongBuffer(neccolBuf.asLongBuffer());

        // Read necrow
        int necrowSize = is.readInt();
        ByteBuffer necrowBuf = ByteBuffer.allocate(necrowSize * 8);
        dataChannel.read(necrowBuf);
        necrowBuf.rewind();
        this.necrow = ThreadSafeLongBuffer.fromLongBuffer(necrowBuf.asLongBuffer());

        // Read rowoffsets
        int rowoffsetsSize = is.readInt();
        ByteBuffer rowoffsetsBuf = ByteBuffer.allocate(rowoffsetsSize * 8);
        dataChannel.read(rowoffsetsBuf);
        rowoffsetsBuf.rewind();
        this.rowoffsets = ThreadSafeLongBuffer.fromLongBuffer(rowoffsetsBuf.asLongBuffer());

        // Read coloffsets
        int coloffsetsSize = is.readInt();
        ByteBuffer coloffsetsBuf = ByteBuffer.allocate(coloffsetsSize * 8);
        dataChannel.read(coloffsetsBuf);
        coloffsetsBuf.rewind();
        this.coloffsets = ThreadSafeLongBuffer.fromLongBuffer(coloffsetsBuf.asLongBuffer());

        // Read celloffsets
        int celloffsetsSize = is.readInt();
        ByteBuffer celloffsetsBuf = ByteBuffer.allocate(celloffsetsSize * 8);
        dataChannel.read(celloffsetsBuf);
        celloffsetsBuf.rewind();
        this.celloffsets = ThreadSafeLongBuffer.fromLongBuffer(celloffsetsBuf.asLongBuffer());

        // Read rowsizes
        int rowsizesSize = is.readInt();
        ByteBuffer rowsizesBuf = ByteBuffer.allocate(rowsizesSize * 4);
        dataChannel.read(rowsizesBuf);
        rowsizesBuf.rewind();
        this.rowsizes = ThreadSafeIntBuffer.fromIntBuffer(rowsizesBuf.asIntBuffer());

        int colsizesSize = is.readInt();
        ByteBuffer colsizesBuf = ByteBuffer.allocate(colsizesSize * 4);
        dataChannel.read(colsizesBuf);
        colsizesBuf.rewind();
        this.colsizes = ThreadSafeIntBuffer.fromIntBuffer(colsizesBuf.asIntBuffer());

        int roffSize = is.readInt();
        ByteBuffer roffBuf = ByteBuffer.allocate(roffSize * 4);
        dataChannel.read(roffBuf);
        roffBuf.rewind();
        this.roff = ThreadSafeIntBuffer.fromIntBuffer(roffBuf.asIntBuffer());

        int coffSize = is.readInt();
        ByteBuffer coffBuf = ByteBuffer.allocate(coffSize * 4);
        dataChannel.read(coffBuf);
        coffBuf.rewind();
        this.coff = ThreadSafeIntBuffer.fromIntBuffer(coffBuf.asIntBuffer());

        int sum = 0;
        wavelettree = new ThreadSafeByteBuffer[contextsSize];
        for (int i = 0; i < contextsSize; i++) {
            int wavelettreeSize = is.readInt();
            sum += wavelettreeSize;
            wavelettree[i] = null;
            if (wavelettreeSize != 0) {
                ByteBuffer wavelettreeBuf = ByteBuffer.allocate(wavelettreeSize);
                dataChannel.read(wavelettreeBuf);
                wavelettree[i] = ThreadSafeByteBuffer.fromByteBuffer(((ByteBuffer) wavelettreeBuf.rewind()));
            }
        }
    }

    private ByteBuffer sliceOrderLimit(ByteBuffer buf, int size) {
        ByteBuffer ret = (ByteBuffer) buf.slice().order(ByteOrder.BIG_ENDIAN).limit(size);
        buf.position(buf.position() + size);
        return ret;
    }

    public void readFromBuffer(ByteBuffer buf) {
        buf.rewind();

        metadata = ThreadSafeByteBuffer.fromByteBuffer(sliceOrderLimit(buf, 36));

        // Deserialize metadata
        originalSize = metadata.getInt();
        sampledSASize = metadata.getInt();
        alphaSize = metadata.getInt();
        sigmaSize = metadata.getInt();
        bits = metadata.getInt();
        sampledSABits = metadata.getInt();
        samplingBase = metadata.getInt();
        samplingRate = metadata.getInt();
        numContexts = metadata.getInt();
        metadata.rewind();

        int alphabetmapSize = alphaSize * (1 + 8 + 4);
        alphabetmap = ThreadSafeByteBuffer.fromByteBuffer(sliceOrderLimit(buf, alphabetmapSize));

        // Deserialize alphabetmap
        alphabetMap = new HashMap<Byte, Pair<Long, Integer>>();
        for (int i = 0; i < this.alphaSize; i++) {
            byte c = alphabetmap.get();
            long v1 = alphabetmap.getLong();
            int v2 = alphabetmap.getInt();
            alphabetMap.put(c, new Pair<Long, Integer>(v1, v2));
        }
        alphabetmap.rewind();

        // Read contexts
        int contextmapSize = numContexts * 8 * 2;
        contextmap = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, contextmapSize).asLongBuffer());

        // Deserialize contexts
        contextMap = new HashMap<Long, Long>();
        for (int i = 0; i < this.numContexts; i++) {
            long v1 = contextmap.get();
            long v2 = contextmap.get();
            contextMap.put(v1, v2);
        }
        contextmap.rewind();

        // Read alphabet
        alphabet = ThreadSafeByteBuffer.fromByteBuffer(sliceOrderLimit(buf, alphaSize));

        // Read sa
        int saSize = ((sampledSASize * sampledSABits) / 64 + 1) * 8;
        sa = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, saSize).asLongBuffer());

        // Read isa
        int isaSize = ((sampledSASize * sampledSABits) / 64 + 1) * 8;
        isa = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, isaSize).asLongBuffer());

        // Read neccol
        int neccolSize = buf.getInt() * 8;
        neccol = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, neccolSize).asLongBuffer());

        // Read necrow
        int necrowSize = buf.getInt() * 8;
        necrow = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, necrowSize).asLongBuffer());

        // Read rowoffsets
        int rowoffsetsSize = buf.getInt() * 8;
        rowoffsets = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, rowoffsetsSize).asLongBuffer());

        // Read coloffsets
        int coloffsetsSize = buf.getInt() * 8;
        coloffsets = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, coloffsetsSize).asLongBuffer());

        // Read celloffsets
        int celloffsetsSize = buf.getInt() * 8;
        celloffsets = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, celloffsetsSize).asLongBuffer());

        // Read rowsizes
        int rowsizesSize = buf.getInt() * 4;
        rowsizes = ThreadSafeIntBuffer.fromIntBuffer(sliceOrderLimit(buf, rowsizesSize).asIntBuffer());

        int colsizesSize = buf.getInt() * 4;
        colsizes = ThreadSafeIntBuffer.fromIntBuffer(sliceOrderLimit(buf, colsizesSize).asIntBuffer());

        int roffSize = buf.getInt() * 4;
        roff = ThreadSafeIntBuffer.fromIntBuffer(sliceOrderLimit(buf, roffSize).asIntBuffer());

        int coffSize = buf.getInt() * 4;
        coff = ThreadSafeIntBuffer.fromIntBuffer(sliceOrderLimit(buf, coffSize).asIntBuffer());

        wavelettree = new ThreadSafeByteBuffer[getNumContexts()];
        for (int i = 0; i < getNumContexts(); i++) {
            int wavelettreeSize = buf.getInt();
            wavelettree[i] = null;
            if (wavelettreeSize != 0) {
                wavelettree[i] = ThreadSafeByteBuffer.fromByteBuffer(sliceOrderLimit(buf, wavelettreeSize));
            }
        }
    }

    public void memoryMap(String path) throws IOException {
        File file = new File(path);
        long size = file.length();
        FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();

        ByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
        readFromBuffer(buf);
    }

    public void readFromFile(String path) throws IOException {
        FileInputStream fis = new FileInputStream(path);
        DataInputStream is = new DataInputStream(fis);
        readFromStream(is);
    }

    public void writeToFile(String path) throws IOException {
        FileOutputStream fos = new FileOutputStream(path);
        DataOutputStream os = new DataOutputStream(fos);
        writeToStream(os);
    }

    protected byte[] toByteArray() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        writeToStream(new DataOutputStream(bos));
        return bos.toByteArray();
    }

    protected void fromByteArray(byte[] data) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        readFromStream(new DataInputStream(bis));
    }

    /**
     * Serialize SuccinctBuffer to OutputStream.
     *
     * @param oos ObjectOutputStream to write to.
     * @throws IOException
     */
    private void writeObject(ObjectOutputStream oos) throws IOException {
        resetBuffers();
        writeToStream(new DataOutputStream(oos));
        resetBuffers();
    }

    /**
     * Deserialize SuccinctBuffer from InputStream.
     *
     * @param ois ObjectInputStream to read from.
     * @throws IOException
     */
    private void readObject(ObjectInputStream ois)
            throws ClassNotFoundException, IOException {
        Tables.init();
        readFromStream(new DataInputStream(ois));
    }

    /**
     * Reposition all byte buffers to their respective starting positions.
     */
    protected void resetBuffers() {
        metadata.order(ByteOrder.BIG_ENDIAN).rewind();
        alphabetmap.order(ByteOrder.BIG_ENDIAN).rewind();
        contextmap.rewind();
        alphabet.rewind();
        sa.rewind();
        isa.rewind();
        neccol.rewind();
        necrow.rewind();
        rowoffsets.rewind();
        coloffsets.rewind();
        celloffsets.rewind();
        rowsizes.rewind();
        colsizes.rewind();
        roff.rewind();
        coff.rewind();
        for(int i = 0; i < wavelettree.length; i++) {
            if(wavelettree[i] != null) {
                wavelettree[i].order(ByteOrder.BIG_ENDIAN).rewind();
            }
        }
    }
}
