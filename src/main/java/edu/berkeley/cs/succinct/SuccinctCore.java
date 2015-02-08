package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.bitmap.BMArray;
import edu.berkeley.cs.succinct.bitmap.BitMap;
import edu.berkeley.cs.succinct.dictionary.Dictionary;
import edu.berkeley.cs.succinct.dictionary.Tables;
import edu.berkeley.cs.succinct.qsufsort.QSufSort;
import edu.berkeley.cs.succinct.util.CommonUtils;
import edu.berkeley.cs.succinct.util.SerializedOperations;
import edu.berkeley.cs.succinct.wavelettree.WaveletTree;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class SuccinctCore implements Serializable {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1382615274437547247L;

    protected ByteBuffer metadata;
    protected ByteBuffer alphabetmap;
    protected LongBuffer contextmap;
    protected ByteBuffer alphabet;
    protected ByteBuffer dbpos;
    protected LongBuffer sa;
    protected LongBuffer isa;
    protected LongBuffer neccol;
    protected LongBuffer necrow;
    protected LongBuffer rowoffsets;
    protected LongBuffer coloffsets;
    protected LongBuffer celloffsets;
    protected IntBuffer rowsizes;
    protected IntBuffer colsizes;
    protected IntBuffer roff;
    protected IntBuffer coff;
    protected ByteBuffer[] wavelettree;

    // Metadata
    private int originalSize;
    protected int sampledSASize;
    protected int alphaSize;
    protected int sigmaSize;
    protected int bits;
    protected int sampledSABits;
    protected int samplingBase;
    protected int samplingRate;
    protected int numContexts;
    protected int contextLen;

    // TODO: Can we serialize these?
    protected HashMap<Byte, Pair<Long, Integer>> alphabetMap;
    protected Map<Long, Long> contextMap;

    public SuccinctCore(byte[] input, int contextLen) {

        this.contextLen = contextLen;

        // Initializing Table data
        Tables.init();

        construct(input);
    }

    public static class Pair<T1, T2> {

        public T1 first;
        public T2 second;

        public Pair(T1 first, T2 second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toString() {
            return "(" + first.toString() + ", " + second.toString() + ")";
        }
    }

    public long lookupNPA(long i) {

        if(i > originalSize || i < 0) {
            throw new ArrayIndexOutOfBoundsException();
        }

        int colId, rowId, cellId, cellOff, contextSize, contextPos;
        long colOff, rowOff;

        // Search columnoffset
        colId = SerializedOperations.getRank1(coloffsets, 0, sigmaSize, i) - 1;

        // Get columnoffset
        colOff = coloffsets.get(colId);

        // Search celloffsets
        cellId = SerializedOperations.getRank1(celloffsets, coff.get(colId),
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
        contextPos = SerializedOperations.getRank1(necrow, roff.get(rowId),
                rowsizes.get(rowId), colId) - 1;

        long cellValue = cellOff;
        if (wavelettree[rowId] != null) {
            cellValue = SerializedOperations.getValueWtree(
                    (ByteBuffer) wavelettree[rowId].position(0), contextPos,
                    cellOff, 0, contextSize - 1);
            wavelettree[rowId].position(0);
        }

        return rowOff + cellValue;
    }

    public long lookupSA(long i) {

        if(i > originalSize || i < 0) {
            throw new ArrayIndexOutOfBoundsException();
        }

        long value = 0, rank, sampledValue;
        while (SerializedOperations.getRank1(dbpos, 0, (int) i)
                - SerializedOperations.getRank1(dbpos, 0, (int) (i - 1)) == 0) {
            i = lookupNPA(i);
            value++;
        }

        rank = CommonUtils.modulo(
                SerializedOperations.getRank1(dbpos, 0, (int) i) - 1,
                getOriginalSize());
        sampledValue = SerializedOperations.getVal(sa, (int) rank,
                sampledSABits);

        return CommonUtils.modulo((samplingRate * sampledValue) - value,
                getOriginalSize());
    }

    public long lookupISA(long i) {

        if(i > originalSize || i < 0) {
            throw new ArrayIndexOutOfBoundsException();
        }

        long sampledValue, pos;
        long v = i % samplingRate;
        sampledValue = SerializedOperations.getVal(isa,
                (int) (i / samplingRate), sampledSABits);
        pos = SerializedOperations.getSelect1(dbpos, 0, (int) sampledValue);
        while (v != 0) {
            pos = lookupNPA(pos);
            v--;
        }

        return pos;
    }

    private int construct(byte[] input) {

        // Collect metadata
        setOriginalSize(input.length);
        bits = CommonUtils.intLog2(getOriginalSize() + 1);
        samplingBase = 5; // Hard coded
        samplingRate = (1 << samplingBase);

        // Construct SA, SAinv
        QSufSort qsuf = new QSufSort();
        qsuf.buildSuffixArray(input);

        BMArray cSA = new BMArray(qsuf.getSA());
        BMArray cSAinv = new BMArray(qsuf.getSAinv());

        this.constructAux(cSA, input);

        sigmaSize = 0; // TODO: Combine sigmaSize and alphaSize
        int alphaBits = CommonUtils.intLog2(alphaSize + 1);
        BMArray textBitMap = new BMArray(getOriginalSize(), alphaBits);
        for (int i = 0; i < getOriginalSize(); i++) {
            textBitMap.setVal(i, alphabetMap.get(input[i]).second);
            sigmaSize = Math.max(alphabetMap.get(input[i]).second, sigmaSize);
        }
        sigmaSize++;

        this.constructPsi(textBitMap, sigmaSize, cSA, cSAinv,
                getOriginalSize(), contextLen);
        this.constructCSA(cSA);

        numContexts = contextMap.size();
        metadata = ByteBuffer.allocate(52);
        metadata.putLong(getOriginalSize());
        metadata.putLong(sampledSASize);
        metadata.putInt(alphaSize);
        metadata.putInt(sigmaSize);
        metadata.putInt(bits);
        metadata.putInt(sampledSABits);
        metadata.putInt(samplingBase);
        metadata.putInt(samplingRate);
        metadata.putInt(numContexts);
        metadata.flip();

        return 0;
    }

    private int constructAux(BMArray SA, byte[] T) {

        ArrayList<Long> CinvIndex = new ArrayList<Long>();
        byte[] alphabetArray;
        alphabetMap = new HashMap<Byte, Pair<Long, Integer>>();
        alphaSize = 1;

        for (long i = 1; i < T.length; ++i) {
            if (T[(int) SA.getVal((int) i)] != T[(int) SA.getVal((int) (i - 1))]) {
                CinvIndex.add(i);
                alphaSize++;
            }
        }
        alphaSize++;

        alphabetArray = new byte[alphaSize];

        alphabetArray[0] = T[(int) SA.getVal(0)];
        alphabetMap.put(alphabetArray[0], new Pair<Long, Integer>(0L, 0));
        long i;
        for (i = 1; i < alphaSize - 1; i++) {
            long sel = CinvIndex.get((int) i - 1);
            alphabetArray[(int) i] = T[(int) SA.getVal((int) sel)];
            alphabetMap.put(alphabetArray[(int) i], new Pair<Long, Integer>(sel, (int) i));
        }
        alphabetMap.put((byte) 0, new Pair<Long, Integer>((long) T.length, (int) i));
        alphabetArray[(int) i] = (char) 0;

        // Serialize cmap
        alphabetmap = ByteBuffer.allocate(alphabetMap.size() * (1 + 8 + 4));
        for (Byte c : alphabetMap.keySet()) {
            Pair<Long, Integer> cval = alphabetMap.get(c);
            alphabetmap.put(c);
            alphabetmap.putLong(cval.first);
            alphabetmap.putInt(cval.second);
        }
        alphabetmap.flip();

        // Serialize S
        alphabet = ByteBuffer.allocate(alphabetArray.length);
        for (int j = 0; j < alphabetArray.length; j++) {
            alphabet.put((byte) alphabetArray[j]);
        }
        alphabet.flip();

        return 0;
    }

    private int constructPsi(BMArray T, int sigma_size, BMArray SA,
            BMArray SAinv, long n, int h) {

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
        for (long i = 0; i < n; i++) {
            long contextValue = getContextVal(T, sigma_size, i, h, n);
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
            sizes[k] = contextSizesMap.get((long) currentContext.getKey());
            starts[k] = n;
            contextMap.put((Long) currentContext.getKey(), (long) k);
            k++;
        }

        assert (k == contextMap.size());
        contextSizesMap.clear();

        BitMap NonNullBitMap = new BitMap((int) (k * sigma_size));
        table = new ArrayList<ArrayList<ArrayList<Long>>>(k);
        cellOffsets = new ArrayList<ArrayList<Long>>();
        necCol = new ArrayList<ArrayList<Long>>();
        colOffsets = new ArrayList<Long>();
        rowOffsets = new ArrayList<Long>();
        necRow = new ArrayList<ArrayList<Long>>();
        wavelettree = new ByteBuffer[k];

        for (int i = 0; i < k; i++) {
            table.add(new ArrayList<ArrayList<Long>>(sigma_size));
            for (int j = 0; j < sigma_size; j++) {
                ArrayList<Long> tableCell = new ArrayList<Long>();
                table.get(i).add(tableCell);
            }
            necRow.add(new ArrayList<Long>());
        }

        for (int i = 0; i < sigma_size; i++) {
            necCol.add(new ArrayList<Long>());
            cellOffsets.add(new ArrayList<Long>());
        }

        k1 = SA.getVal(0);
        contextId = (int) getContextVal(T, sigma_size, (k1 + 1) % n, h, n);
        contextVal = contextMap.get((long) contextId).intValue();
        npaVal = SAinv.getVal((int) ((SA.getVal(0) + 1) % n));
        table.get(contextVal).get((int) (lOff / k)).add(npaVal);
        starts[contextVal] = Math.min(starts[contextVal], npaVal);
        c_sizes[contextVal]++;

        NonNullBitMap.setBit((int) contextVal);
        necCol.get(0).add((long) contextVal);
        necColSize++;
        colOffsets.add(0L);
        cellOffsets.get(0).add(0L);
        cellOffsetsSize++;

        for (long i = 1; i < n; i++) {
            k1 = SA.getVal((int) i);
            k2 = SA.getVal((int) i - 1);

            contextId = (int) getContextVal(T, sigma_size, (k1 + 1) % n, h, n);
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
            } else if (!compareT(T, (k1 + 1) % n, (k2 + 1) % n, h)) {
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
            npaVal = SAinv.getVal((int) ((SA.getVal((int) i) + 1) % n));

            assert (lOff / k < sigma_size);
            // Push npa value to npa table: note indices. indexed by context
            // value, and l_offs / k
            table.get(contextVal).get((int) (lOff / k)).add(npaVal);
            starts[contextVal] = Math.min(starts[contextVal], npaVal);
            if (table.get(contextVal).get((int) (lOff / k)).size() == 1) {
                c_sizes[contextVal]++;
            }
        }

        for (int i = 0; i < k; i++) {
            flag = false;
            for (int j = 0; j < sigma_size; j++) {
                if (!flag && NonNullBitMap.getBit((int) (i + j * k)) == 1) {
                    rowOffsets.add(p);
                    p += table.get((int) i).get((int) j).size();
                    flag = true;
                } else if (NonNullBitMap.getBit(i + j * k) == 1) {
                    p += table.get((int) i).get((int) j).size();
                }

                if (NonNullBitMap.getBit((int) (i + j * k)) == 1) {
                    cell = new ArrayList<Long>();
                    for (long t = 0; t < table.get((int) i).get((int) j).size(); t++) {
                        cell.add(table.get((int) i).get((int) j).get((int) t)
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

            WaveletTree wtree = new WaveletTree(0, context.size() - 1,
                    contextValues, contextColumnIds);
            wavelettree[i] = wtree.getByteBuffer();

            contextValues.clear();
            contextColumnIds.clear();
            context.clear();
        }
        table.clear();

        // Serialize neccol
        neccol = LongBuffer.allocate(necColSize);
        for (int i = 0; i < necCol.size(); i++) {
            for (int j = 0; j < necCol.get(i).size(); j++) {
                neccol.put(necCol.get(i).get(j));
            }
        }

        neccol.flip();

        // Serialize necrow
        necrow = LongBuffer.allocate(necRowSize);
        for (int i = 0; i < necRow.size(); i++) {
            for (int j = 0; j < necRow.get(i).size(); j++) {
                necrow.put(necRow.get(i).get(j));
            }
        }
        necrow.flip();

        // Serialize rowoffsets
        rowoffsets = LongBuffer.allocate(rowOffsets.size());
        for (int i = 0; i < rowOffsets.size(); i++) {
            rowoffsets.put(rowOffsets.get(i));
        }
        rowoffsets.flip();

        // Serialize coloffsets
        coloffsets = LongBuffer.allocate(colOffsets.size());
        for (int i = 0; i < colOffsets.size(); i++) {
            coloffsets.put(colOffsets.get(i));
        }
        coloffsets.flip();

        // Serialize celloffsets
        celloffsets = LongBuffer.allocate(cellOffsetsSize);
        for (int i = 0; i < cellOffsets.size(); i++) {
            for (int j = 0; j < cellOffsets.get(i).size(); j++) {
                celloffsets.put(cellOffsets.get(i).get(j));
            }
        }
        celloffsets.flip();

        // Serialize rowsizes
        rowsizes = IntBuffer.allocate(necRow.size());
        for (int i = 0; i < necRow.size(); i++) {
            rowsizes.put(necRow.get(i).size());
        }
        rowsizes.flip();

        // Serialize colsizes
        colsizes = IntBuffer.allocate(necCol.size());
        for (int i = 0; i < necCol.size(); i++) {
            colsizes.put(necCol.get(i).size());
        }
        colsizes.flip();

        // Serialize roff
        int size = 0;
        roff = IntBuffer.allocate(necRow.size());
        for (int i = 0; i < necRow.size(); i++) {
            roff.put(size);
            size += necRow.get(i).size();
        }
        roff.flip();

        // Serialize coff
        size = 0;
        coff = IntBuffer.allocate(necCol.size());
        for (int i = 0; i < necCol.size(); i++) {
            coff.put(size);
            size += necCol.get(i).size();
        }
        coff.flip();

        // Serialize contexts
        contextmap = LongBuffer.allocate(2 * contextMap.size());
        for (Long c : contextMap.keySet()) {
            contextmap.put(c);
            contextmap.put(contextMap.get(c));
        }
        contextmap.flip();

        rowOffsets = null;
        colOffsets = null;
        cellOffsets = null;
        necRow = null;
        necCol = null;

        return 0;
    }

    private boolean compareT(BMArray T, long i, long j, int k) {

        for (long p = i; p < i + k; p++) {
            if (T.getVal((int) p) != T.getVal((int) j++)) {
                return false;
            }
        }

        return true;
    }

    private long getContextVal(BMArray T, int sigma_size, long i, int k, long n) {

        long val = 0;
        long max = Math.min(i + k, n);
        for (long p = i; p < max; p++) {
            val = val * sigma_size + T.getVal((int) p);
        }

        if (max < i + k) {
            for (long p = 0; p < (i + k) % n; p++) {
                val = val * sigma_size + T.getVal((int) p);
            }
        }

        return val;
    }

    private int constructCSA(BMArray cSA) {

        samplingRate = (1 << samplingBase);
        sampledSASize = (getOriginalSize() / samplingRate) + 1;
        sampledSABits = CommonUtils.intLog2(sampledSASize + 1);

        long saValue, sampedPos = 0;
        BitMap cBPos = new BitMap(getOriginalSize());

        BMArray SA = new BMArray((int) sampledSASize, sampledSABits);

        for (int i = 0; i < getOriginalSize(); i++) {
            saValue = cSA.getVal(i);
            if (saValue % samplingRate == 0) {
                SA.setVal((int) sampedPos++, saValue / samplingRate);
                cBPos.setBit(i);
            }
        }

        Dictionary DBPos = new Dictionary(cBPos);
        cBPos = null;

        System.gc();

        BMArray SAinv = new BMArray((int) sampledSASize, sampledSABits);

        for (int i = 0; i < sampledSASize; i++) {
            SAinv.setVal((int) SA.getVal(i), i);
        }

        System.gc();

        // Serialize SA
        sa = LongBuffer.wrap(SA.data);

        // Serialize SAinv
        isa = LongBuffer.wrap(SAinv.data);

        // Serialize BPos
        int dbposSize = 8 * (1 + DBPos.rankL3.length + DBPos.rankL12.length
                + DBPos.posL3.length + DBPos.posL12.length
                + DBPos.bitMap.data.length + 1);
        dbpos = ByteBuffer.allocate(dbposSize);
        dbpos.putLong(DBPos.size);
        for (int i = 0; i < DBPos.rankL3.length; i++) {
            dbpos.putLong(DBPos.rankL3[i]);
        }
        for (int i = 0; i < DBPos.posL3.length; i++) {
            dbpos.putLong(DBPos.posL3[i]);
        }
        for (int i = 0; i < DBPos.rankL12.length; i++) {
            dbpos.putLong(DBPos.rankL12[i]);
        }
        for (int i = 0; i < DBPos.posL12.length; i++) {
            dbpos.putLong(DBPos.posL12[i]);
        }
        dbpos.putLong(DBPos.bitMap.size);
        for (int i = 0; i < DBPos.bitMap.data.length; i++) {
            dbpos.putLong(DBPos.bitMap.data[i]);
        }
        dbpos.flip();

        DBPos.bitMap.data = null;
        DBPos.bitMap = null;
        DBPos.rankL3 = null;
        DBPos.posL3 = null;
        DBPos.rankL12 = null;
        DBPos.posL12 = null;
        DBPos = null;
        System.gc();

        return 0;
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        WritableByteChannel dataChannel = Channels.newChannel(oos);

        // System.out.println("metadata size = " + metadata.capacity());
        dataChannel.write(metadata.order(ByteOrder.nativeOrder()));

        // System.out.println("cmap size = " + cmap.capacity());
        dataChannel.write(alphabetmap.order(ByteOrder.nativeOrder()));

        // System.out.println("contxt size = " + contxt.capacity());
        ByteBuffer bufContext = ByteBuffer.allocate(contextmap.capacity() * 8);
        bufContext.asLongBuffer().put(contextmap);
        dataChannel.write(bufContext.order(ByteOrder.nativeOrder()));

        // System.out.println("slist size = " + slist.capacity());
        dataChannel.write(alphabet.order(ByteOrder.nativeOrder()));

        // System.out.println("dbpos size = " + dbpos.capacity());
        oos.writeLong((long) dbpos.capacity());
        dataChannel.write(dbpos.order(ByteOrder.nativeOrder()));

        // System.out.println("sa size = " + sa.capacity());
        ByteBuffer bufSA = ByteBuffer.allocate(sa.capacity() * 8);
        bufSA.asLongBuffer().put(sa);
        dataChannel.write(bufSA.order(ByteOrder.nativeOrder()));

        // System.out.println("isa size = " + sainv.capacity());
        ByteBuffer bufISA = ByteBuffer.allocate(isa.capacity() * 8);
        bufISA.asLongBuffer().put(isa);
        dataChannel.write(bufISA.order(ByteOrder.nativeOrder()));

        // System.out.println("neccol size = " + neccol.capacity());
        oos.writeLong((long) neccol.capacity());
        ByteBuffer bufNecCol = ByteBuffer.allocate(neccol.capacity() * 8);
        bufNecCol.asLongBuffer().put(neccol);
        dataChannel.write(bufNecCol.order(ByteOrder.nativeOrder()));

        // System.out.println("necrow size = " + necrow.capacity());
        oos.writeLong((long) necrow.capacity());
        ByteBuffer bufNecRow = ByteBuffer.allocate(necrow.capacity() * 8);
        bufNecRow.asLongBuffer().put(necrow);
        dataChannel.write(bufNecRow.order(ByteOrder.nativeOrder()));

        // System.out.println("rowoffsets size = " + rowoffsets.capacity());
        oos.writeLong((long) rowoffsets.capacity());
        ByteBuffer bufRowOff = ByteBuffer.allocate(rowoffsets.capacity() * 8);
        bufRowOff.asLongBuffer().put(rowoffsets);
        dataChannel.write(bufRowOff.order(ByteOrder.nativeOrder()));

        // System.out.println("coloffsets size = " + coloffsets.capacity());
        oos.writeLong((long) coloffsets.capacity());
        ByteBuffer bufColOff = ByteBuffer.allocate(coloffsets.capacity() * 8);
        bufColOff.asLongBuffer().put(coloffsets);
        dataChannel.write(bufColOff.order(ByteOrder.nativeOrder()));

        // System.out.println("celloffsets size = " + celloffsets.capacity());
        oos.writeLong((long) celloffsets.capacity());
        ByteBuffer bufCellOff = ByteBuffer.allocate(celloffsets.capacity() * 8);
        bufCellOff.asLongBuffer().put(celloffsets);
        dataChannel.write(bufCellOff.order(ByteOrder.nativeOrder()));

        // System.out.println("rowsizes size = " + rowsizes.capacity());
        oos.writeLong((long) rowsizes.capacity());
        ByteBuffer bufRowSizes = ByteBuffer.allocate(rowsizes.capacity() * 4);
        bufRowSizes.asIntBuffer().put(rowsizes);
        dataChannel.write(bufRowSizes.order(ByteOrder.nativeOrder()));

        // System.out.println("colsizes size = " + colsizes.capacity());
        oos.writeLong((long) colsizes.capacity());
        ByteBuffer bufColSizes = ByteBuffer.allocate(colsizes.capacity() * 4);
        bufColSizes.asIntBuffer().put(colsizes);
        dataChannel.write(bufColSizes.order(ByteOrder.nativeOrder()));

        // System.out.println("roff size = " + roff.capacity());
        oos.writeLong((long) roff.capacity());
        ByteBuffer bufROff = ByteBuffer.allocate(roff.capacity() * 4);
        bufROff.asIntBuffer().put(roff);
        dataChannel.write(bufROff.order(ByteOrder.nativeOrder()));

        // System.out.println("coff size = " + coff.capacity());
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
    }

    private void readObject(ObjectInputStream ois)
            throws ClassNotFoundException, IOException {

        Tables.init();

        ReadableByteChannel dataChannel = Channels.newChannel(ois);
        this.setOriginalSize((int) ois.readLong());
        // System.out.println("originalSize = " + originalSize);
        this.sampledSASize = (int) ois.readLong();
        // System.out.println("sampledSASize = " + sampledSASize);
        this.alphaSize = ois.readInt();
        // System.out.println("alphaSize = " + alphaSize);
        this.sigmaSize = ois.readInt();
        // System.out.println("sigmaSize = " + sigmaSize);
        this.bits = ois.readInt();
        // System.out.println("bits = " + bits);
        this.sampledSABits = ois.readInt();
        // System.out.println("sampledSABits = " + sampledSABits);
        this.samplingBase = ois.readInt();
        // System.out.println("samplingBase = " + samplingBase);
        this.samplingRate = ois.readInt();
        // System.out.println("samplingRate = " + samplingRate);
        this.numContexts = ois.readInt();
        // System.out.println("numContexts = " + numContexts);

        metadata = ByteBuffer.allocate(52);
        metadata.putLong(getOriginalSize());
        metadata.putLong(sampledSASize);
        metadata.putInt(alphaSize);
        metadata.putInt(sigmaSize);
        metadata.putInt(bits);
        metadata.putInt(sampledSABits);
        metadata.putInt(samplingBase);
        metadata.putInt(samplingRate);
        metadata.putInt(numContexts);
        metadata.flip();

        int cmapSize = this.alphaSize;
        // System.out.println("Cmap size = " + cmapSize);
        this.alphabetmap = ByteBuffer.allocate(cmapSize * (1 + 8 + 4));
        dataChannel.read(this.alphabetmap);
        this.alphabetmap.flip();

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
        // System.out.println("Contexts size = " + contextsSize);
        ByteBuffer contextBuf = ByteBuffer.allocate(contextsSize * 8 * 2);
        dataChannel.read(contextBuf);
        contextBuf.flip();
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
        // System.out.println("Slist size = " + slistSize);
        this.alphabet = ByteBuffer.allocate(slistSize);
        dataChannel.read(this.alphabet);
        this.alphabet.flip();

        // Read dbpos
        int dbposSize = (int) ois.readLong();
        // System.out.println("Dbpos size = " + dbposSize);
        this.dbpos = ByteBuffer.allocate(dbposSize);
        dataChannel.read(this.dbpos);
        this.dbpos.flip();

        // Read sa
        int saSize = (int) ((sampledSASize * sampledSABits) / 64) + 1;
        // System.out.println("SA size = " + saSize);
        ByteBuffer saBuf = ByteBuffer.allocate(saSize * 8);
        dataChannel.read(saBuf);
        saBuf.flip();
        this.sa = saBuf.asLongBuffer();

        // Read sainv
        int isaSize = (int) ((sampledSASize * sampledSABits) / 64) + 1;
        // System.out.println("ISA size = " + isaSize);
        ByteBuffer isaBuf = ByteBuffer.allocate(isaSize * 8);
        dataChannel.read(isaBuf);
        isaBuf.flip();
        this.isa = isaBuf.asLongBuffer();

        // Read neccol
        int neccolSize = (int) ois.readLong();
        // System.out.println("neccol size = " + neccolSize);
        ByteBuffer neccolBuf = ByteBuffer.allocate(neccolSize * 8);
        dataChannel.read(neccolBuf);
        neccolBuf.flip();
        this.neccol = neccolBuf.asLongBuffer();

        // Read necrow
        int necrowSize = (int) ois.readLong();
        // System.out.println("necrow size = " + necrowSize);
        ByteBuffer necrowBuf = ByteBuffer.allocate(necrowSize * 8);
        dataChannel.read(necrowBuf);
        necrowBuf.flip();
        this.necrow = necrowBuf.asLongBuffer();

        // Read rowoffsets
        int rowoffsetsSize = (int) ois.readLong();
        // System.out.println("rowoffsets size = " + rowoffsetsSize);
        ByteBuffer rowoffsetsBuf = ByteBuffer.allocate(rowoffsetsSize * 8);
        dataChannel.read(rowoffsetsBuf);
        rowoffsetsBuf.flip();
        this.rowoffsets = rowoffsetsBuf.asLongBuffer();

        // Read coloffsets
        int coloffsetsSize = (int) ois.readLong();
        // System.out.println("coloffsets size = " + coloffsetsSize);
        ByteBuffer coloffsetsBuf = ByteBuffer.allocate(coloffsetsSize * 8);
        dataChannel.read(coloffsetsBuf);
        coloffsetsBuf.flip();
        this.coloffsets = coloffsetsBuf.asLongBuffer();

        // Read celloffsets
        int celloffsetsSize = (int) ois.readLong();
        // System.out.println("celloffsets size = " + celloffsetsSize);
        ByteBuffer celloffsetsBuf = ByteBuffer.allocate(celloffsetsSize * 8);
        dataChannel.read(celloffsetsBuf);
        celloffsetsBuf.flip();
        this.celloffsets = celloffsetsBuf.asLongBuffer();

        // Read rowsizes
        int rowsizesSize = (int) ois.readLong();
        // System.out.println("rowsizes size = " + rowsizesSize);
        ByteBuffer rowsizesBuf = ByteBuffer.allocate(rowsizesSize * 4);
        dataChannel.read(rowsizesBuf);
        rowsizesBuf.flip();
        this.rowsizes = rowsizesBuf.asIntBuffer();

        int colsizesSize = (int) ois.readLong();
        // System.out.println("colsizes size = " + colsizesSize);
        ByteBuffer colsizesBuf = ByteBuffer.allocate(colsizesSize * 4);
        dataChannel.read(colsizesBuf);
        colsizesBuf.flip();
        this.colsizes = colsizesBuf.asIntBuffer();

        int roffSize = (int) ois.readLong();
        // System.out.println("roff size = " + roffSize);
        ByteBuffer roffBuf = ByteBuffer.allocate(roffSize * 4);
        dataChannel.read(roffBuf);
        roffBuf.flip();
        this.roff = roffBuf.asIntBuffer();

        int coffSize = (int) ois.readLong();
        // System.out.println("coff size = " + coffSize);
        ByteBuffer coffBuf = ByteBuffer.allocate(coffSize * 4);
        dataChannel.read(coffBuf);
        coffBuf.flip();
        this.coff = coffBuf.asIntBuffer();

        wavelettree = new ByteBuffer[contextsSize];
        // System.out.println("contexts size = " + contextsSize);
        for (int i = 0; i < contextsSize; i++) {
            long wavelettreeSize = ois.readLong();
            // System.out.println("Size = " + wavelettreeSize);
            wavelettree[i] = null;
            if (wavelettreeSize != 0) {
                ByteBuffer wavelettreeBuf = ByteBuffer
                        .allocate((int) wavelettreeSize);
                dataChannel.read(wavelettreeBuf);
                wavelettree[i] = (ByteBuffer) wavelettreeBuf.flip();
            }
        }
    }

    /**
     * @return the originalSize
     */
    public int getOriginalSize() {
        return originalSize;
    }

    /**
     * @param originalSize
     *            the originalSize to set
     */
    public void setOriginalSize(int originalSize) {
        this.originalSize = originalSize;
    }
}
