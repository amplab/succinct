package edu.berkeley.cs.succinct.dictionary;

import edu.berkeley.cs.succinct.bitmap.BitMap;
import edu.berkeley.cs.succinct.util.CommonUtils;

import java.util.ArrayList;

public class Dictionary {

    public BitMap bitMap;
    public long size;
    public long[] rankL12;
    public long[] rankL3;
    public long[] posL12;
    public long[] posL3;

    public Dictionary(BitMap bitMap) {

        long two32 = (1L << 32);
        int l3_size = (int) ((bitMap.size / two32) + 1);
        int l2_size = (int) ((bitMap.size / 2048) + 1);
        int l1_size = (int) ((bitMap.size / 512) + 1);
        long count = 0;

        size = bitMap.size;
        rankL3 = new long[l3_size];
        posL3 = new long[l3_size];
        rankL12 = new long[l2_size];
        posL12 = new long[l2_size];
        long[] rank_l2 = new long[l2_size];
        long[] pos_l2 = new long[l2_size];
        long[] rank_l1 = new long[l1_size];
        long[] pos_l1 = new long[l1_size];
        rankL3[0] = 0;
        posL3[0] = 0;
        rankL12[0] = 0;
        posL12[0] = 0;
        int sum_l1 = 0, sum_pos_l1 = 0;
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
                rank_l2[i / 2048] = count - rankL3[(int) (i / two32)];
                pos_l2[i / 2048] = compSize - posL3[(int) (i / two32)];
                rankL12[i / 2048] = rank_l2[i / 2048] << 32;
                posL12[i / 2048] = pos_l2[i / 2048] << 31;
                flag = 0;
                sum_l1 = 0;
                sum_pos_l1 = 0;
            }
            if (i % 512 == 0) {
                rank_l1[i / 512] = count - rank_l2[i / 2048] - sum_l1;
                pos_l1[i / 512] = compSize - pos_l2[i / 2048] - sum_pos_l1;
                sum_l1 += rank_l1[i / 512];
                sum_pos_l1 += pos_l1[i / 512];
                if (flag != 0) {
                    rankL12[i / 2048] |= (rank_l1[i / 512] << (32 - flag * 10));
                    posL12[i / 2048] |= (pos_l1[i / 512] << (31 - flag * 10));
                }
                flag++;
            }
            if (bitMap.getBit(i) == 1) {
                count++;
            }
            if (i % 64 == 0) {
                p = (short) CommonUtils
                        .popcount((bitMap.data[i / 64] >> 48) & 65535);
                p = (short) (p % 16);
                compSize += Tables.offsetBits[p];
                dict.add(p);
                dict.add(Tables.encodeTable.get(p).get(
                        (int) ((bitMap.data[i / 64] >> 48) & 65535)));

                p = (short) CommonUtils
                        .popcount((bitMap.data[i / 64] >> 32) & 65535);
                p = (short) (p % 16);
                compSize += Tables.offsetBits[p];
                dict.add(p);
                dict.add(Tables.encodeTable.get(p).get(
                        (int) ((bitMap.data[i / 64] >> 32) & 65535)));

                p = (short) CommonUtils
                        .popcount((bitMap.data[i / 64] >> 16) & 65535);
                p = (short) (p % 16);
                compSize += Tables.offsetBits[p];
                dict.add(p);
                dict.add(Tables.encodeTable.get(p).get(
                        (int) ((bitMap.data[i / 64] >> 16) & 65535)));

                p = (short) CommonUtils.popcount(bitMap.data[i / 64] & 65535);
                p = (short) (p % 16);
                compSize += Tables.offsetBits[p];
                dict.add(p);
                dict.add(Tables.encodeTable.get(p).get(
                        (int) (bitMap.data[i / 64] & 65535)));

                compSize += 16;
            }
        }

        this.bitMap = new BitMap((int) compSize);
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

    private long GETRANKL2(long n) {
        return (n >>> 32);
    }

    private long GETRANKL1(long n, int i) {
        return (((n & 0xffffffff) >>> (32 - i * 10)) & 0x3ff);
    }

    private long GETPOSL2(long n) {
        return (n >>> 31);
    }

    private long GETPOSL1(long n, int i) {
        return (((n & 0x7fffffff) >>> (31 - i * 10)) & 0x3ff);
    }

    public long getRank1(int query) {
        if (query < 0)
            return 0;

        int l3Idx = (int) (query / CommonUtils.two32);
        int l2Idx = query / 2048;
        int l1Idx = (query % 512);
        int rem = ((query % 2048) / 512);
        int blockClass, blockOffset;

        long res = this.rankL3[l3Idx] + GETRANKL2(rankL12[l2Idx]);
        long pos = this.posL3[l3Idx] + GETPOSL2(posL12[l2Idx]);

        switch (rem) {
        case 1:
            res += GETRANKL1(rankL12[l2Idx], 1);
            pos += GETPOSL1(posL12[l2Idx], 1);
            break;

        case 2:
            res += GETRANKL1(rankL12[l2Idx], 1) + GETRANKL1(rankL12[l2Idx], 2);
            pos += GETPOSL1(posL12[l2Idx], 1) + GETPOSL1(posL12[l2Idx], 2);
            break;

        case 3:
            res += GETRANKL1(rankL12[l2Idx], 1) + GETRANKL1(rankL12[l2Idx], 2)
                    + GETRANKL1(rankL12[l2Idx], 3);
            pos += GETPOSL1(posL12[l2Idx], 1) + GETPOSL1(posL12[l2Idx], 2)
                    + GETPOSL1(posL12[l2Idx], 3);
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

    public long getRank0(int i) {
        return i - getRank1(i) + 1;
    }
}
