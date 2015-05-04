package edu.berkeley.cs.succinct.dictionary;

import edu.berkeley.cs.succinct.bitmap.BitMap;
import edu.berkeley.cs.succinct.util.CommonUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Dictionary {

    public BitMap bitMap;
    public long size;
    public long[] rankL12;
    public long[] rankL3;
    public long[] posL12;
    public long[] posL3;

    /**
     * Constructor to create a Dictionary from an input BitMap.
     *
     * @param bitMap Bitmap to convert to dictionary.
     */
    public Dictionary(BitMap bitMap) {

        long two32 = (1L << 32);
        int l3Size = (int) ((bitMap.size / two32) + 1);
        int l2Size = (int) ((bitMap.size / 2048) + 1);
        int l1Size = (int) ((bitMap.size / 512) + 1);
        long count = 0;

        size = bitMap.size;
        rankL3 = new long[l3Size];
        posL3 = new long[l3Size];
        rankL12 = new long[l2Size];
        posL12 = new long[l2Size];
        long[] rankL2 = new long[l2Size];
        long[] posL2 = new long[l2Size];
        long[] rankL1 = new long[l1Size];
        long[] posL1 = new long[l1Size];
        rankL3[0] = 0;
        posL3[0] = 0;
        rankL12[0] = 0;
        posL12[0] = 0;
        int sumL1 = 0, sumPosL1 = 0;
        int i;
        int flag = 0;
        int compSize = 0;
        int p;
        ArrayList<Integer> dict = new ArrayList<Integer>();
        for (i = 0; i < bitMap.size; i++) {
            if (i % two32 == 0) {
                rankL3[(int) (i / two32)] = count;
                posL3[(int) (i / two32)] = compSize;
            }
            if (i % 2048 == 0) {
                rankL2[i / 2048] = count - rankL3[(int) (i / two32)];
                posL2[i / 2048] = compSize - posL3[(int) (i / two32)];
                rankL12[i / 2048] = rankL2[i / 2048] << 32;
                posL12[i / 2048] = posL2[i / 2048] << 31;
                flag = 0;
                sumL1 = 0;
                sumPosL1 = 0;
            }
            if (i % 512 == 0) {
                rankL1[i / 512] = count - rankL2[i / 2048] - sumL1;
                posL1[i / 512] = compSize - posL2[i / 2048] - sumPosL1;
                sumL1 += rankL1[i / 512];
                sumPosL1 += posL1[i / 512];
                if (flag != 0) {
                    rankL12[i / 2048] |= (rankL1[i / 512] << (32 - flag * 10));
                    posL12[i / 2048] |= (posL1[i / 512] << (31 - flag * 10));
                }
                flag++;
            }
            if (bitMap.getBit(i) == 1) {
                count++;
            }
            if (i % 64 == 0) {
                p = (short) CommonUtils
                        .popCount((bitMap.data[i / 64] >> 48) & 65535);
                p = (short) (p % 16);
                compSize += Tables.offsetBits[p];
                dict.add(p);
                dict.add(Tables.encodeTable.get(p).get(
                        (int) ((bitMap.data[i / 64] >> 48) & 65535)));

                p = (short) CommonUtils
                        .popCount((bitMap.data[i / 64] >> 32) & 65535);
                p = (short) (p % 16);
                compSize += Tables.offsetBits[p];
                dict.add(p);
                dict.add(Tables.encodeTable.get(p).get(
                        (int) ((bitMap.data[i / 64] >> 32) & 65535)));

                p = (short) CommonUtils
                        .popCount((bitMap.data[i / 64] >> 16) & 65535);
                p = (short) (p % 16);
                compSize += Tables.offsetBits[p];
                dict.add(p);
                dict.add(Tables.encodeTable.get(p).get(
                        (int) ((bitMap.data[i / 64] >> 16) & 65535)));

                p = (short) CommonUtils.popCount(bitMap.data[i / 64] & 65535);
                p = (short) (p % 16);
                compSize += Tables.offsetBits[p];
                dict.add(p);
                dict.add(Tables.encodeTable.get(p).get(
                        (int) (bitMap.data[i / 64] & 65535)));

                compSize += 16;
            }
        }

        this.bitMap = new BitMap(compSize);
        long numBits = 0;
        for (i = 0; i < dict.size(); i++) {
            if (i % 2 == 0) {
                this.bitMap.setValPos((int) numBits, dict.get(i), 4);
                numBits += 4;
            } else {
                this.bitMap.setValPos((int) numBits, dict.get(i),
                        Tables.offsetBits[dict.get(i - 1)]);
                numBits += Tables.offsetBits[dict.get(i - 1)];
            }
        }
    }

    /**
     * Method to perform rank1 using dictionary index.
     *
     * @param i Position in the bitmap.
     * @return Value of rank1 at specified position.
     */
    public long getRank1(int i) {
        if (i < 0)
            return 0;

        int l3Idx = (int) (i / CommonUtils.two32);
        int l2Idx = i / 2048;
        int l1Idx = (i % 512);
        int rem = ((i % 2048) / 512);
        int blockClass, blockOffset;

        long res = this.rankL3[l3Idx] + CommonUtils.DictionaryUtils.GETRANKL2(rankL12[l2Idx]);
        long pos = this.posL3[l3Idx] + CommonUtils.DictionaryUtils.GETPOSL2(posL12[l2Idx]);

        switch (rem) {
            case 1:
                res += CommonUtils.DictionaryUtils.GETRANKL1(rankL12[l2Idx], 1);
                pos += CommonUtils.DictionaryUtils.GETPOSL1(posL12[l2Idx], 1);
                break;

            case 2:
                res += CommonUtils.DictionaryUtils.GETRANKL1(rankL12[l2Idx], 1) + CommonUtils.DictionaryUtils.GETRANKL1(rankL12[l2Idx], 2);
                pos += CommonUtils.DictionaryUtils.GETPOSL1(posL12[l2Idx], 1) + CommonUtils.DictionaryUtils.GETPOSL1(posL12[l2Idx], 2);
                break;

            case 3:
                res += CommonUtils.DictionaryUtils.GETRANKL1(rankL12[l2Idx], 1) + CommonUtils.DictionaryUtils.GETRANKL1(rankL12[l2Idx], 2)
                        + CommonUtils.DictionaryUtils.GETRANKL1(rankL12[l2Idx], 3);
                pos += CommonUtils.DictionaryUtils.GETPOSL1(posL12[l2Idx], 1) + CommonUtils.DictionaryUtils.GETPOSL1(posL12[l2Idx], 2)
                        + CommonUtils.DictionaryUtils.GETPOSL1(posL12[l2Idx], 3);
                break;

            default:
                break;
        }

        // Popcount
        while (l1Idx >= 16) {
            blockClass = (int) bitMap.getValPos((int) pos, 4);
            pos += 4;
            blockOffset = (int) ((blockClass == 0) ? bitMap.getBit((int) pos) * 16
                    : 0);
            pos += Tables.offsetBits[blockClass];
            res += blockClass + blockOffset;
            l1Idx -= 16;
        }

        blockClass = (int) bitMap.getValPos((int) pos, 4);
        pos += 4;
        blockOffset = (int) bitMap.getValPos((int) pos,
                Tables.offsetBits[blockClass]);
        res += Tables.smallRank[Tables.decodeTable[blockClass][blockOffset]][l1Idx];

        return res;
    }

    /**
     * Method to perform rank0 using dictionary index.
     *
     * @param i Position in the bitmap.
     * @return Value of rank0 at specified position.
     */
    public long getRank0(int i) {
        return i - getRank1(i) + 1;
    }

    /**
     * Serialize Dictionary as a ByteBuffer.
     *
     * @return Serialized Dictionary as a ByteBuffer.
     */
    public ByteBuffer getByteBuffer() {
        int dSize = 8 * (1 + rankL3.length + rankL12.length
                + posL3.length + posL12.length
                + bitMap.data.length + 1);
        
        ByteBuffer dBuf = ByteBuffer.allocate(dSize);
        dBuf.putLong(size);
        for (int i = 0; i < rankL3.length; i++) {
            dBuf.putLong(rankL3[i]);
        }
        for (int i = 0; i < posL3.length; i++) {
            dBuf.putLong(posL3[i]);
        }
        for (int i = 0; i < rankL12.length; i++) {
            dBuf.putLong(rankL12[i]);
        }
        for (int i = 0; i < posL12.length; i++) {
            dBuf.putLong(posL12[i]);
        }
        dBuf.putLong(bitMap.size);
        for (int i = 0; i < bitMap.data.length; i++) {
            dBuf.putLong(bitMap.data[i]);
        }
        dBuf.flip();
        
        return dBuf;
    }
}
