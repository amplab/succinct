/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package succinct;

import succinct.dictionary.Tables;
import succinct.regex.executor.RegExExecutor;
import succinct.regex.parser.RegEx;
import succinct.regex.parser.RegExParser;
import succinct.regex.parser.RegExParsingException;
import succinct.regex.planner.NaiveRegExPlanner;
import succinct.regex.planner.RegExPlanner;
import succinct.util.SerializedOperations;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SuccinctBuffer extends SuccinctCore {

    /**
	 * 
	 */
    private static final long serialVersionUID = 5879363803993345049L;

    public SuccinctBuffer(byte[] input, int contextLen) {
        super(input, contextLen);
    }

    public SuccinctBuffer(byte[] input) {
        this(input, 3);
    }

    private long computeContextVal(char[] p, int sigma_size, int i, int k) {
        long val = 0;
        long max = i + k;
        for (int t = i; t < max; t++) {
            if (alphabetMap.containsKey((byte) p[t])) {
                val = val * sigma_size + alphabetMap.get((byte) p[t]).second;
            } else {
                return -1;
            }
        }

        return val;
    }

    /* Extract portion of text starting at offset, with length len */
    public String extract(int i, int len) {

        char[] buf = new char[len];
        long s;

        s = lookupISA(i);
        for (int k = 0; k < len; k++) {
            buf[k] = (char) alphabet.get(SerializedOperations.getRank1(
                    coloffsets, 0, sigmaSize, s) - 1);
            s = lookupNPA(s);
        }

        return new String(buf);
    }

    public String extractUntil(int i, char delim) {

        String strBuf = "";
        long s;

        s = lookupISA(i);
        char nextChar;
        do {
            nextChar = (char) alphabet.get(SerializedOperations.getRank1(
                    coloffsets, 0, sigmaSize, s) - 1);
            if(nextChar == delim || nextChar == 1) break;
            strBuf += nextChar;
            s = lookupNPA(s);
        } while(true);

        return strBuf;
    }

    /* Binary search */
    private long binSearchPsi(long val, long s, long e, boolean flag) {

        long sp = s;
        long ep = e;
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

    /* Get range of SA positions using Slow Backward search */
    private Pair<Long, Long> getRangeSlow(char[] p) {
        Pair<Long, Long> range = new Pair<Long, Long>(0L, -1L);
        int m = p.length;
        long sp, ep, c1, c2;

        if (alphabetMap.containsKey((byte) p[m - 1])) {
            sp = alphabetMap.get((byte) p[m - 1]).first;
            ep = alphabetMap
                    .get((alphabet.get(alphabetMap.get((byte) p[m - 1]).second + 1))).first - 1;
        } else {
            return range;
        }

        if (sp > ep) {
            return range;
        }

        for (int i = m - 2; i >= 0; i--) {
            if (alphabetMap.containsKey((byte) p[i])) {
                c1 = alphabetMap.get((byte) p[i]).first;
                c2 = alphabetMap
                        .get((alphabet.get(alphabetMap.get((byte) p[i]).second + 1))).first - 1;
            } else {
                return range;
            }
            sp = binSearchPsi(sp, c1, c2, false);
            ep = binSearchPsi(ep, c1, c2, true);
            if (sp > ep) {
                return range;
            }
        }

        range.first = sp;
        range.second = ep;

        return range;
    }

    /* Get range of SA positions using Backward search */
    protected Pair<Long, Long> getRange(char[] p) {

        int m = p.length;
        if (m <= contextLen) {
            return getRangeSlow(p);
        }
        Pair<Long, Long> range = new Pair<Long, Long>(0L, -1L);
        int sigma_id;
        long sp, ep, c1, c2;
        int start_off;
        long context_val, context_id;

        if (alphabetMap.containsKey((byte) p[m - contextLen - 1])) {
            sigma_id = alphabetMap.get((byte) p[m - contextLen - 1]).second;
            context_val = computeContextVal(p, sigmaSize, m - contextLen,
                    contextLen);

            if (context_val == -1) {
                return range;
            }
            if (!contextMap.containsKey(context_val)) {
                return range;
            }

            context_id = contextMap.get(context_val);
            start_off = SerializedOperations.getRank1(neccol,
                    coff.get(sigma_id), colsizes.get(sigma_id), context_id) - 1;
            sp = coloffsets.get(sigma_id)
                    + celloffsets.get(coff.get(sigma_id) + start_off);
            if (start_off + 1 < colsizes.get(sigma_id)) {
                ep = coloffsets.get(sigma_id)
                        + celloffsets.get(coff.get(sigma_id) + start_off + 1)
                        - 1;
            } else if (sigma_id + 1 < sigmaSize) {
                ep = coloffsets.get(sigma_id + 1) - 1;
            } else {
                ep = getOriginalSize() - 1;
            }
        } else {
            return range;
        }

        if (sp > ep) {
            return range;
        }

        for (int i = m - contextLen - 2; i >= 0; i--) {
            if (alphabetMap.containsKey((byte) p[i])) {
                sigma_id = alphabetMap.get((byte) p[i]).second;
                context_val = computeContextVal(p, sigmaSize, i + 1, contextLen);

                if (context_val == -1) {
                    return range;
                }
                if (!contextMap.containsKey(context_val)) {
                    return range;
                }

                context_id = contextMap.get(context_val);
                start_off = SerializedOperations.getRank1(neccol,
                        coff.get(sigma_id), colsizes.get(sigma_id), context_id) - 1;
                c1 = coloffsets.get(sigma_id)
                        + celloffsets.get(coff.get(sigma_id) + start_off);

                if (start_off + 1 < colsizes.get(sigma_id)) {
                    c2 = coloffsets.get(sigma_id)
                            + celloffsets.get(coff.get(sigma_id) + start_off
                                    + 1) - 1;
                } else if (sigma_id + 1 < sigmaSize) {
                    c2 = coloffsets.get(sigma_id + 1) - 1;
                } else {
                    c2 = getOriginalSize() - 1;
                }
            } else {
                return range;
            }
            sp = binSearchPsi(sp, c1, c2, false);
            ep = binSearchPsi(ep, c1, c2, true);
            if (sp > ep) {
                return range;
            }
        }
        range.first = sp;
        range.second = ep;

        return range;
    }

    /* Get count of pattern occurrances */
    public long count(String query) {
        char[] p = query.toCharArray();
        Pair<Long, Long> range;
        range = getRange(p);
        return range.second - range.first + 1;
    }

    /* Function for backward search */
    public List<Long> search(String query) {

        char[] p = query.toCharArray();
        Pair<Long, Long> range;
        range = getRange(p);

        long sp = range.first, ep = range.second;
        if (ep - sp + 1 <= 0) {
            return new ArrayList<Long>();
        }

        List<Long> positions = new ArrayList<Long>();
        for (long i = 0; i < ep - sp + 1; i++) {
            positions.add(lookupSA(sp + i));
        }

        return positions;
    }

    public Map<Long, Integer> regexSearch(String query) throws RegExParsingException {
        RegExParser parser = new RegExParser(query);
        RegEx regEx;

        regEx = parser.parse();

        RegExPlanner planner = new NaiveRegExPlanner(this, regEx);
        RegEx optRegEx = planner.plan();

        RegExExecutor regExExecutor = new RegExExecutor(this, optRegEx);
        regExExecutor.execute();

        return regExExecutor.getFinalResults();
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
}
