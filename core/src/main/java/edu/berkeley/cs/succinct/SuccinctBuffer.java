package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.dictionary.Tables;
import edu.berkeley.cs.succinct.regex.executor.RegExExecutor;
import edu.berkeley.cs.succinct.regex.parser.RegEx;
import edu.berkeley.cs.succinct.regex.parser.RegExParser;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.regex.planner.NaiveRegExPlanner;
import edu.berkeley.cs.succinct.regex.planner.RegExPlanner;
import edu.berkeley.cs.succinct.util.SerializedOperations;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
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
     * Compute context value at an index in a byte array.
     *  
     * @param buf Input byte buffer.
     * @param i Index in the byte buffer.
     * @return Value of context at specified index.
     */
    private long computeContextVal(byte[] buf, int i) {
        long val = 0;
        long max = i + contextLen;
        for (int t = i; t < max; t++) {
            if (alphabetMap.containsKey(buf[t])) {
                val = val * sigmaSize + alphabetMap.get(buf[t]).second;
            } else {
                return -1;
            }
        }

        return val;
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
                    coloffsets, 0, sigmaSize, s) - 1);
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
                    coloffsets, 0, sigmaSize, s) - 1);
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
    public Range<Long, Long> getRange(byte[] buf) {
        Range<Long, Long> range = new Range<Long, Long>(0L, -1L);
        int m = buf.length;
        long c1, c2;

        if (alphabetMap.containsKey(buf[m - 1])) {
            range.first = alphabetMap.get(buf[m - 1]).first;
            range.second = alphabetMap
                    .get((alphabet.get(alphabetMap.get(buf[m - 1]).second + 1))).first - 1;
        } else {
            return range;
        }

        if (range.first > range.second) {
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
            if (range.first > range.second) {
                return range;
            }
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
        Range<Long, Long> range;
        range = getRange(query);
        return range.second - range.first + 1;
    }

    /**
     * Get all locations of pattern occurrences in original input.
     *
     * @param query Input query.
     * @return All locations of pattern occurrences in original input.
     */
    public Long[] search(byte[] query) {

        Range<Long, Long> range;
        range = getRange(query);

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

    /**
     * Serialize SuccinctBuffer to OutputStream.
     *
     * @param oos ObjectOutputStream to write to.
     * @throws IOException
     */
    private void writeObject(ObjectOutputStream oos) throws IOException {
        resetBuffers();
        
        WritableByteChannel dataChannel = Channels.newChannel(oos);

        dataChannel.write(metadata.order(ByteOrder.nativeOrder()));

        dataChannel.write(alphabetmap.order(ByteOrder.nativeOrder()));

        ByteBuffer bufContext = ByteBuffer.allocate(contextmap.capacity() * 8);
        bufContext.asLongBuffer().put(contextmap);
        dataChannel.write(bufContext.order(ByteOrder.nativeOrder()));

        dataChannel.write(alphabet.order(ByteOrder.nativeOrder()));

        oos.writeLong((long) dbpos.capacity());
        dataChannel.write(dbpos.order(ByteOrder.nativeOrder()));

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
     * Deserialize SuccinctBuffer from InputStream.
     *
     * @param ois ObjectInputStream to read from.
     * @throws IOException
     */
    private void readObject(ObjectInputStream ois)
            throws ClassNotFoundException, IOException {

        Tables.init();

        ReadableByteChannel dataChannel = Channels.newChannel(ois);
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

        // Read dbpos
        int dbposSize = (int) ois.readLong();
        this.dbpos = ByteBuffer.allocate(dbposSize);
        dataChannel.read(this.dbpos);
        this.dbpos.position(0);

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
