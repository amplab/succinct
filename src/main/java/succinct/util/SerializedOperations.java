package succinct.util;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import succinct.dictionary.Tables;

public class SerializedOperations {

    // TESTED
    public static long getValueWtree(ByteBuffer wtree, int contextPos,
            int cellPos, int s, int e) {

        char m = (char) wtree.get();
        int left = (int) wtree.getLong();
        int right = (int) wtree.getLong();
        int dictPos = wtree.position();
        long p, v;

        if (contextPos > m && contextPos <= e) {
            if (right == 0) {
                return SerializedOperations.getSelect1(wtree, dictPos, cellPos);
            }
            p = getValueWtree((ByteBuffer) wtree.position(right), contextPos,
                    cellPos, m + 1, e);
            v = SerializedOperations.getSelect1(wtree, dictPos, (int) p);
        } else {
            if (left == 0) {
                return SerializedOperations.getSelect0(wtree, dictPos, cellPos);
            }
            p = getValueWtree((ByteBuffer) wtree.position(left), contextPos,
                    cellPos, s, m);
            v = SerializedOperations.getSelect0(wtree, dictPos, (int) p);
        }

        return v;
    }

    // TESTED
    public static int getRank1(LongBuffer B, int startPos, int size, long i) {
        int sp = 0, ep = size - 1;
        int m;

        while (sp <= ep) {
            m = (sp + ep) / 2;
            if (B.get(startPos + m) == i) {
                return m + 1;
            } else if (i < B.get(startPos + m)) {
                ep = m - 1;
            } else {
                sp = m + 1;
            }
        }

        return ep + 1;
    }

    // TESTED
    public static long getRank1(ByteBuffer buf, int startPos, int query) {
        if (query < 0) {
            return 0;
        }

        int l3Idx = (int) (query / CommonUtils.two32);
        int l2Idx = query / 2048;
        int l1Idx = (query % 512);
        int rem = ((query % 2048) / 512);
        int blockClass, blockOffset;

        buf.position(startPos);
        LongBuffer dictBuf = buf.asLongBuffer();
        long size = dictBuf.get();

        int l3_size = (int) (size / CommonUtils.two32) + 1;
        int l12_size = (int) (size / 2048) + 1;

        int basePos = dictBuf.position();

        long rank_l3 = dictBuf.get(basePos + l3Idx);
        long pos_l3 = dictBuf.get(basePos + l3_size + l3Idx);
        long rank_l12 = dictBuf.get(basePos + l3_size + l3_size + l2Idx);
        long pos_l12 = dictBuf.get(basePos + l3_size + l3_size + l12_size
                + l2Idx);
        dictBuf.position(basePos + l3_size + l3_size + l12_size + l12_size);

        long res = rank_l3 + GETRANKL2(rank_l12);
        long pos = pos_l3 + GETPOSL2(pos_l12);

        switch (rem) {
        case 1:
            res += GETRANKL1(rank_l12, 1);
            pos += GETPOSL1(pos_l12, 1);
            break;

        case 2:
            res += GETRANKL1(rank_l12, 1) + GETRANKL1(rank_l12, 2);
            pos += GETPOSL1(pos_l12, 1) + GETPOSL1(pos_l12, 2);
            break;

        case 3:
            res += GETRANKL1(rank_l12, 1) + GETRANKL1(rank_l12, 2)
                    + GETRANKL1(rank_l12, 3);
            pos += GETPOSL1(pos_l12, 1) + GETPOSL1(pos_l12, 2)
                    + GETPOSL1(pos_l12, 3);
            break;

        default:
            break;
        }

        dictBuf.get(); // TODO: Could remove this field altogether

        // Popcount
        while (l1Idx >= 16) {
            blockClass = (int) SerializedOperations.getValPos(dictBuf,
                    (int) pos, 4);
            pos += 4;
            blockOffset = (int) ((blockClass == 0) ? SerializedOperations
                    .getBit(dictBuf, (int) pos) * 16 : 0);
            pos += Tables.offsetBits[blockClass];
            res += blockClass + blockOffset;
            l1Idx -= 16;
        }

        blockClass = (int) SerializedOperations
                .getValPos(dictBuf, (int) pos, 4);
        pos += 4;
        blockOffset = (int) SerializedOperations.getValPos(dictBuf, (int) pos,
                Tables.offsetBits[blockClass]);
        res += Tables.smallRank[Tables.decodeTable[blockClass][blockOffset]][l1Idx];

        return res;
    }

    // TESTED
    public static long getRank0(ByteBuffer buf, int startPos, int i) {
        return i - getRank1(buf, startPos, i) + 1;
    }

    public static long getSelect1(ByteBuffer buf, int startPos, int i) {

        assert (i >= 0);

        buf.position(startPos);
        LongBuffer dictBuf = buf.asLongBuffer();

        long size = dictBuf.get();

        long val = i + 1;
        int sp = 0;
        int ep = (int) (size / CommonUtils.two32);
        int m;
        long r;
        int pos = 0;
        int blockClass, blockOffset;
        long sel = 0;
        int lastblock;
        long rankL12, posL12;

        int l3Size = (int) ((size / CommonUtils.two32) + 1);
        int l12Size = (int) ((size / 2048) + 1);
        int basePos = dictBuf.position();

        while (sp <= ep) {
            m = (sp + ep) / 2;
            r = dictBuf.get(basePos + m);
            if (val > r) {
                sp = m + 1;
            } else {
                ep = m - 1;
            }
        }

        ep = Math.max(ep, 0);
        val -= dictBuf.get(basePos + ep);
        pos += dictBuf.get(basePos + l3Size + ep);
        sp = (int) (ep * CommonUtils.two32 / 2048);
        ep = (int) (Math.min(((ep + 1) * CommonUtils.two32 / 2048),
                Math.ceil((double) size / 2048.0)) - 1);

        dictBuf.position(basePos + 2 * l3Size);
        basePos = dictBuf.position();

        while (sp <= ep) {
            m = (sp + ep) / 2;
            r = GETRANKL2(dictBuf.get(basePos + m));
            if (val > r) {
                sp = m + 1;
            } else {
                ep = m - 1;
            }
        }

        ep = Math.max(ep, 0);
        sel = (long) (ep) * 2048L;
        rankL12 = dictBuf.get(basePos + ep);
        posL12 = dictBuf.get(basePos + l12Size + ep);
        val -= GETRANKL2(rankL12);
        pos += GETPOSL2(posL12);

        assert (val <= 2048);
        r = GETRANKL1(rankL12, 1);
        if (sel + 512 < size && val > r) {
            pos += GETPOSL1(posL12, 1);
            val -= r;
            sel += 512;
            r = GETRANKL1(rankL12, 2);
            if (sel + 512 < size && val > r) {
                pos += GETPOSL1(posL12, 2);
                val -= r;
                sel += 512;
                r = GETRANKL1(rankL12, 3);
                if (sel + 512 < size && val > r) {
                    pos += GETPOSL1(posL12, 3);
                    val -= r;
                    sel += 512;
                }
            }
        }

        dictBuf.position(basePos + 2 * l12Size);

        assert (val <= 512);
        dictBuf.get(); // TODO: Could remove this field altogether

        while (true) {
            blockClass = (int) getValPos(dictBuf, pos, 4);
            short tempint = (short) Tables.offsetBits[blockClass];
            pos += 4;
            blockOffset = (int) ((blockClass == 0) ? getBit(dictBuf, pos) * 16
                    : 0);
            pos += tempint;

            if (val <= (blockClass + blockOffset)) {
                pos -= (4 + tempint);
                break;
            }

            val -= (blockClass + blockOffset);
            sel += 16;
        }

        blockClass = (int) getValPos(dictBuf, pos, 4);
        pos += 4;
        blockOffset = (int) getValPos(dictBuf, pos,
                Tables.offsetBits[blockClass]);
        lastblock = Tables.decodeTable[blockClass][blockOffset];

        long count = 0;
        for (i = 0; i < 16; i++) {
            if (((lastblock >>> (15 - i)) & 1) == 1) {
                count++;
            }
            if (count == val) {
                return sel + i;
            }
        }

        return sel;
    }

    public static long getSelect0(ByteBuffer buf, int startPos, int i) {

        assert (i >= 0);

        buf.position(startPos);
        LongBuffer dictBuf = buf.asLongBuffer();

        long size = dictBuf.get();

        long val = i + 1;
        int sp = 0;
        int ep = (int) (size / CommonUtils.two32);
        int m;
        long r;
        int pos = 0;
        int block_class, block_offset;
        long sel = 0;
        int lastblock;
        long rank_l12, pos_l12;

        int l3_size = (int) ((size / CommonUtils.two32) + 1);
        int l12_size = (int) ((size / 2048) + 1);

        int basePos = dictBuf.position();

        while (sp <= ep) {
            m = (sp + ep) / 2;
            r = (m * CommonUtils.two32 - dictBuf.get(basePos + m));
            if (val > r) {
                sp = m + 1;
            } else {
                ep = m - 1;
            }
        }

        ep = Math.max(ep, 0);
        val -= (ep * CommonUtils.two32 - dictBuf.get(basePos + ep));
        pos += dictBuf.get(basePos + l3_size + ep);
        sp = (int) (ep * CommonUtils.two32 / 2048);
        ep = (int) (Math.min(((ep + 1) * CommonUtils.two32 / 2048),
                Math.ceil((double) size / 2048.0)) - 1);

        dictBuf.position(basePos + 2 * l3_size);
        basePos = dictBuf.position();

        while (sp <= ep) {
            m = (sp + ep) / 2;
            r = m * 2048 - GETRANKL2(dictBuf.get(basePos + m));
            if (val > r) {
                sp = m + 1;
            } else {
                ep = m - 1;
            }
        }

        ep = Math.max(ep, 0);
        sel = (long) (ep) * 2048L;
        rank_l12 = dictBuf.get(basePos + ep);
        pos_l12 = dictBuf.get(basePos + l12_size + ep);
        val -= (ep * 2048 - GETRANKL2(rank_l12));
        pos += GETPOSL2(pos_l12);

        assert (val <= 2048);
        r = (512 - GETRANKL1(rank_l12, 1));
        if (sel + 512 < size && val > r) {
            pos += GETPOSL1(pos_l12, 1);
            val -= r;
            sel += 512;
            r = (512 - GETRANKL1(rank_l12, 2));
            if (sel + 512 < size && val > r) {
                pos += GETPOSL1(pos_l12, 2);
                val -= r;
                sel += 512;
                r = (512 - GETRANKL1(rank_l12, 3));
                if (sel + 512 < size && val > r) {
                    pos += GETPOSL1(pos_l12, 3);
                    val -= r;
                    sel += 512;
                }
            }
        }

        dictBuf.position(basePos + 2 * l12_size);

        assert (val <= 512);
        dictBuf.get(); // TODO: Could remove this field altogether

        while (true) {
            block_class = (int) getValPos(dictBuf, pos, 4);
            short tempint = (short) Tables.offsetBits[block_class];
            pos += 4;
            block_offset = (int) ((block_class == 0) ? getBit(dictBuf, pos) * 16
                    : 0);
            pos += tempint;

            if (val <= (16 - (block_class + block_offset))) {
                pos -= (4 + tempint);
                break;
            }

            val -= (16 - (block_class + block_offset));
            sel += 16;
        }

        block_class = (int) getValPos(dictBuf, pos, 4);
        pos += 4;
        block_offset = (int) getValPos(dictBuf, pos,
                Tables.offsetBits[block_class]);
        lastblock = Tables.decodeTable[block_class][block_offset];

        long count = 0;
        for (i = 0; i < 16; i++) {
            if (((lastblock >> (15 - i)) & 1) == 0) {
                count++;
            }
            if (count == val) {
                return sel + i;
            }
        }

        return sel;
    }

    public static long getVal(LongBuffer B, int i, int bits) {
        assert (i >= 0);

        long val;
        long s = (long) (i) * bits;
        long e = s + (bits - 1);

        if ((s / 64) == (e / 64)) {
            val = B.get((int) (s / 64L)) << (s % 64);
            val = val >>> (63 - e % 64 + s % 64);
        } else {
            long val1 = B.get((int) (s / 64L)) << (s % 64);
            long val2 = B.get((int) (e / 64L)) >>> (63 - e % 64);
            val1 = val1 >>> (s % 64 - (e % 64 + 1));
            val = val1 | val2;
        }

        return val;
    }

    public static long GETPOSL1(long n, int i) {
        return (((n & 0x7fffffff) >>> (31 - i * 10)) & 0x3ff);
    }

    public static long GETPOSL2(long n) {
        return (n >>> 31);
    }

    public static long GETRANKL1(long n, int i) {
        return (((n & 0xffffffff) >>> (32 - i * 10)) & 0x3ff);
    }

    public static long GETRANKL2(long n) {
        return (n >>> 32);
    }

    public static long getValPos(long bitmap[], int pos, int bits) {
        assert (pos >= 0);

        long val;
        long s = (long) pos;
        long e = s + (bits - 1);

        if ((s / 64) == (e / 64)) {
            val = bitmap[(int) (s / 64L)] << (s % 64);
            val = val >>> (63 - e % 64 + s % 64);
        } else {
            val = bitmap[(int) (s / 64L)] << (s % 64);
            val = val >>> (s % 64 - (e % 64 + 1));
            val = val | (bitmap[(int) (e / 64L)] >>> (63 - e % 64));
        }
        assert (val >= 0);
        return val;
    }

    public static long getValPos(LongBuffer bitmap, int pos, int bits) {
        assert (pos >= 0);

        int basePos = bitmap.position();
        long val;
        long s = (long) pos;
        long e = s + (bits - 1);

        if ((s / 64) == (e / 64)) {
            val = bitmap.get(basePos + (int) (s / 64L)) << (s % 64);
            val = val >>> (63 - e % 64 + s % 64);
        } else {
            val = bitmap.get(basePos + (int) (s / 64L)) << (s % 64);
            val = val >>> (s % 64 - (e % 64 + 1));
            val = val
                    | (bitmap.get(basePos + (int) (e / 64L)) >>> (63 - e % 64));
        }
        assert (val >= 0);
        return val;
    }

    public static long getBit(long bitmap[], int i) {
        return ((bitmap[i / 64] >>> (63L - i)) & 1L);
    }

    public static long getBit(LongBuffer bitmap, int i) {
        return ((bitmap.get(bitmap.position() + (i / 64)) >>> (63L - i)) & 1L);
    }

}
