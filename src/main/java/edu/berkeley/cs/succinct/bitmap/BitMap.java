package edu.berkeley.cs.succinct.bitmap;

public class BitMap {

    public long[] data;
    public long size;

    public BitMap(long n) {
        long bmSize = ((n / 64) + 1);
        this.data = new long[(int) bmSize];
        this.size = n;
    }

    public int bitmapSize() {
        return this.data.length;
    }

    public long getBit(int i) {
        return ((data[i / 64] >>> (63L - i)) & 1L);
    }

    public void setBit(int i) {
        this.data[i / 64] |= (1L << (63L - i));
    }

    public void setValPos(int pos, long val, int bits) {

        assert (pos >= 0 && pos < this.size);
        assert (val >= 0);

        long s = pos;
        long e = s + (bits - 1);
        if ((s / 64) == (e / 64)) {
            this.data[(int) (s / 64L)] |= (val << (63L - e % 64));
        } else {
            this.data[(int) (s / 64L)] |= (val >>> (e % 64 + 1));
            this.data[(int) (e / 64L)] |= (val << (63L - e % 64));
        }
    }

    public long getValPos(int pos, int bits) {
        assert (pos >= 0 && pos < (this.size));

        long val;
        long s = (long) pos;
        long e = s + (bits - 1);

        if ((s / 64) == (e / 64)) {
            val = this.data[(int) (s / 64L)] << (s % 64);
            val = val >>> (63 - e % 64 + s % 64);
        } else {
            val = this.data[(int) (s / 64L)] << (s % 64);
            val = val >>> (s % 64 - (e % 64 + 1));
            val = val | (this.data[(int) (e / 64L)] >>> (63 - e % 64));
        }
        assert (val >= 0);
        return val;
    }

    public void display() {
        for (int i = 0; i < this.size; i++) {
            System.out.print(this.getBit(i));
        }
        System.out.println();
    }

    // Very inefficient!
    public long getSelect1(int i) {
        long sel = -1, count = 0;
        for (int k = 0; k < this.size; ++k) {
            if (this.getBit(k) == 1)
                count++;
            if (count == (i + 1)) {
                sel = k;
                break;
            }
        }

        return sel;
    }

    // Very inefficient
    public long getSelect0(int i) {
        long sel = -1, count = 0;
        for (int k = 0; k < this.size; ++k) {
            if (this.getBit(k) == 0)
                count++;
            if (count == (i + 1)) {
                sel = k;
                break;
            }
        }
        return sel;
    }

    // Very inefficient
    public long getRank1(int i) {
        long count = 0;
        for (int k = 0; k <= i; k++) {
            if (this.getBit(k) == 1)
                count++;
        }
        return count;
    }

    // Very inefficient
    public long getRank0(int i) {
        long count = 0;
        for (int k = 0; k <= i; k++) {
            if (this.getBit(k) == 0)
                count++;
        }
        return count;
    }

    public void clean() {
        this.data = null;
        System.gc();
    }
}
