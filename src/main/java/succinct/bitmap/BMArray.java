package succinct.bitmap;

import succinct.util.CommonUtils;

public class BMArray extends BitMap {

    public int bits;
    public int n;

    public BMArray(int n, int bits) {
        super((long) (((long) n) * ((long) bits)));

        this.n = n;
        this.bits = bits;
    }

    public BMArray(int[] input, int n) {
        super((long) (((long) n) * ((long) CommonUtils.intLog2(n))));
        this.n = n;
        this.bits = CommonUtils.intLog2(this.n + 1);
        for (int i = 0; i < this.n; i++) {
            this.setVal(i, input[i]);
        }
    }

    public final void setVal(int i, long val) {

        assert (i >= 0 && i < this.n);
        assert (val >= 0);

        long s = (long) (i) * this.bits;
        long e = s + (this.bits - 1);
        if ((s / 64) == (e / 64)) {
            this.data[(int) (s / 64L)] |= (val << (63L - e % 64));
        } else {
            this.data[(int) (s / 64L)] |= (val >>> (e % 64 + 1));
            this.data[(int) (e / 64L)] |= (val << (63L - e % 64));
        }
    }

    public final long getVal(int i) {

        assert (i >= 0 && i < (this.n));

        long val;
        long s = (long) (i) * this.bits;
        long e = s + (this.bits - 1);

        if ((s / 64) == (e / 64)) {
            val = this.data[(int) (s / 64L)] << (s % 64);
            val = val >>> (63 - e % 64 + s % 64);
        } else {
            long val1 = this.data[(int) (s / 64L)] << (s % 64);
            long val2 = this.data[(int) (e / 64L)] >>> (63 - e % 64);
            val1 = val1 >>> (s % 64 - (e % 64 + 1));
            val = val1 | val2;
        }

        return val;
    }

}
