package edu.berkeley.cs.succinct.qsufsort;

public class QSufSort {
    /** group array, ultimately suffix array. */
    private int I[];

    /** inverse array, ultimately inverse of I. */
    private int V[];

    /** number of symbols aggregated by transform. */
    private int r;

    /** length of already-sorted prefixes. */
    private int h;

    private int start;

    /**
     * {@inheritDoc}
     * <p>
     * Additional constraints enforced by qsufsort algorithm:
     * <ul>
     * <li>non-negative (&ge;0) symbols in the input</li>
     * <li>length >= 2</li>
     * </ul>
     * <p>
     */

    final static class MinMax {
        public final int min;
        public final int max;

        MinMax(int min, int max) {
            this.min = min;
            this.max = max;
        }

        public int range() {
            return max - min;
        }
    }

    /**
     * Calculate minimum and maximum value for an array.
     */
    static MinMax minmax(byte[] input) {
        int max = input[0];
        int min = max;

        for (int i = 1; i < input.length; i++) {
            final int v = input[i];
            if (v > max)
                max = v;
            if (v < min)
                min = v;
        }

        return new MinMax(min, max);
    }

    public final void buildSuffixArray(byte[] input) {
        MinMax minmax = minmax(input);
        I = new int[input.length + 1];
        V = new int[input.length + 1];
        this.start = 0;
        for (int i = 0; i < input.length; i++)
            V[i] = input[i];
        suffixsort(input.length, minmax.max + 1, minmax.min);
    }

    public final int[] getSA() {
        return I;
    }

    public final int[] getSAinv() {
        return V;
    }

    public final void cleanSA() {
        I = null;
        System.gc();
    }

    public final void cleanSAinv() {
        V = null;
        System.gc();
    }

    /**
     * Makes suffix array {@link #I} of {@link #V}. <code>V</code> becomes
     * inverse of <code>I</code>.
     * <p>
     * Contents of <code>V[0...n-1]</code> are integers in the range
     * <code>l...k-1</code>. Original contents of <code>x[n]</code> is
     * disregarded, the <code>n</code> -th symbol being regarded as
     * end-of-string smaller than all other symbols.
     */
    private void suffixsort(int n, int k, int l) {
        int pi, pk; // I pointers
        int i, j, s, sl;

        if (n >= k - l) { /* if bucketing possible, */
            j = transform(n, k, l, n);
            bucketsort(n, j); /* bucketsort on first r positions. */
        } else {
            transform(n, k, l, Integer.MAX_VALUE);
            for (i = 0; i <= n; ++i)
                I[i] = i; /* initialize I with suffix numbers. */
            h = 0;
            sort_split(0, n + 1); /* quicksort on first r positions. */
        }
        h = r; /* number of symbols aggregated by transform. */
        while (I[0] >= -n) {
            pi = 0; /* pi is first position of group. */
            sl = 0; /* sl is negated length of sorted groups. */
            do {
                if ((s = I[pi]) < 0) {
                    pi -= s; /* skip over sorted group. */
                    sl += s; /* add negated length to sl. */
                } else {
                    if (sl != 0) {
                        I[pi + sl] = sl; /* combine sorted groups before pi. */
                        sl = 0;
                    }
                    pk = V[start + s] + 1; /*
                                            * pk-1 is last position of unsorted
                                            * group.
                                            */
                    sort_split(pi, pk - pi);
                    pi = pk; /* next group. */
                }
            } while (pi <= n);
            if (sl != 0) /* if the array ends with a sorted group. */
                I[pi + sl] = sl; /* combine sorted groups at end of I. */
            h = 2 * h; /* double sorted-depth. */
        }

        for (i = 0; i <= n; ++i) {
            /* reconstruct suffix array from inverse. */
            if (V[start + i] > 0) {
                I[V[start + i] - 1] = i;
            }
        }
    }

    /**
     * Sorting routine called for each unsorted group. Sorts the array
     * {@link #I} integers (suffix numbers) of length <code>n</code> starting at
     * <code>p</code>.
     * <p>
     * The algorithm is a ternary-split quicksort taken from Bentley & McIlroy,
     * "Engineering a Sort Function", Software -- Practice and Experience
     * 23(11), 1249-1265 (November 1993). This function is based on Program 7.
     */
    private void sort_split(int p, int n) {
        int pa, pb, pc, pd, pl, pm, pn;// pointers
        int f, v, s, t;

        if (n < 7) { /* multi-selection sort smallest arrays. */
            select_sort_split(p, n);
            return;
        }

        v = choose_pivot(p, n);
        pa = pb = p;
        pc = pd = p + n - 1;
        while (true) { /* split-end partition. */
            while (pb <= pc && (f = KEY(pb)) <= v) {
                if (f == v) {
                    SWAP(pa, pb);
                    ++pa;
                }
                ++pb;
            }
            while (pc >= pb && (f = KEY(pc)) >= v) {
                if (f == v) {
                    SWAP(pc, pd);
                    --pd;
                }
                --pc;
            }
            if (pb > pc)
                break;
            SWAP(pb, pc);
            ++pb;
            --pc;
        }
        pn = p + n;
        if ((s = pa - p) > (t = pb - pa))
            s = t;
        for (pl = p, pm = pb - s; s != 0; --s, ++pl, ++pm)
            SWAP(pl, pm);
        if ((s = pd - pc) > (t = pn - pd - 1))
            s = t;
        for (pl = pb, pm = pn - s; s != 0; --s, ++pl, ++pm)
            SWAP(pl, pm);

        s = pb - pa;
        t = pd - pc;
        if (s > 0)
            sort_split(p, s);
        update_group(p + s, p + n - t - 1);
        if (t > 0)
            sort_split(p + n - t, t);
    }

    /**
     * Subroutine for {@link #select_sort_split(int, int)} and
     * {@link #sort_split(int, int)}. Sets group numbers for a group whose
     * lowest position in {@link #I} is <code>pl</code> and highest position is
     * <code>pm</code>.
     */
    private void update_group(int pl, int pm) {
        int g;

        g = pm; /* group number. */
        V[start + I[pl]] = g; /* update group number of first position. */
        if (pl == pm)
            I[pl] = -1; /* one element, sorted group. */
        else
            do
                /* more than one element, unsorted group. */
                V[start + I[++pl]] = g; /* update group numbers. */
            while (pl < pm);

    }

    /**
     * Subroutine for {@link #sort_split(int, int)} , algorithm by Bentley &
     * McIlroy.
     */
    private int choose_pivot(int p, int n) {
        int pl, pm, pn;// pointers
        int s;

        pm = p + (n >> 1); /* small arrays, middle element. */
        if (n > 7) {
            pl = p;
            pn = p + n - 1;
            if (n > 40) { /* big arrays, pseudomedian of 9. */
                s = n >> 3;
                pl = MED3(pl, pl + s, pl + s + s);
                pm = MED3(pm - s, pm, pm + s);
                pn = MED3(pn - s - s, pn - s, pn);
            }
            pm = MED3(pl, pm, pn); /* midsize arrays, median of 3. */
        }
        return KEY(pm);
    }

    /**
     * Quadratic sorting method to use for small subarrays. To be able to update
     * group numbers consistently, a variant of selection sorting is used.
     */
    private void select_sort_split(int p, int n) {
        int pa, pb, pi, pn;
        int f, v;

        pa = p; /* pa is start of group being picked out. */
        pn = p + n - 1; /* pn is last position of subarray. */
        while (pa < pn) {
            for (pi = pb = pa + 1, f = KEY(pa); pi <= pn; ++pi)
                if ((v = KEY(pi)) < f) {
                    f = v; /* f is smallest key found. */
                    SWAP(pi, pa); /* place smallest element at beginning. */
                    pb = pa + 1; /* pb is position for elements equal to f. */
                } else if (v == f) { /* if equal to smallest key. */
                    SWAP(pi, pb); /* place next to other smallest elements. */
                    ++pb;
                }
            update_group(pa, pb - 1); /* update group values for new group. */
            pa = pb; /* continue sorting rest of the subarray. */
        }
        if (pa == pn) { /* check if last part is single element. */
            V[start + I[pa]] = pa;
            I[pa] = -1; /* sorted group. */
        }
    }

    /**
     * Bucketsort for first iteration.
     * <p>
     * Input: <code>V[0...n-1]</code> holds integers in the range
     * <code>1...k-1</code>, all of which appear at least once.
     * <code>V[n]</code> is <code>0</code>. (This is the corresponding output of
     * transform.) <code>k</code> must be at most <code>n+1</code>.
     * <code>I</code> is array of size <code>n+1</code> whose contents are
     * disregarded.
     */
    private void bucketsort(int n, int k) {
        int pi;// pointer
        int i, c, d, g;

        for (pi = 0; pi < k; ++pi)
            I[pi] = -1; /* mark linked lists empty. */
        for (i = 0; i <= n; ++i) {
            V[start + i] = I[c = V[start + i]]; /* insert in linked list. */
            I[c] = i;
        }
        for (pi = k - 1, i = n; pi >= 0; --pi) {
            d = V[start + (c = I[pi])]; /* c is position, d is next in list. */
            V[start + c] = g = i; /* last position equals group number. */
            if (d >= 0) { /* if more than one element in group. */
                I[i--] = c; /* p is permutation for the sorted x. */
                do {
                    d = V[start + (c = d)]; /* next in linked list. */
                    V[start + c] = g; /* group number in x. */
                    I[i--] = c; /* permutation in p. */
                } while (d >= 0);
            } else
                I[i--] = -1; /* one element, sorted group. */
        }
    }

    /**
     * Transforms the alphabet of {@link #V} by attempting to aggregate several
     * symbols into one, while preserving the suffix order of <code>V</code>.
     * The alphabet may also be compacted, so that <code>V</code> on output
     * comprises all integers of the new alphabet with no skipped numbers.
     * <p>
     * Input: <code>V</code> is an array of size <code>n+1</code> whose first
     * <code>n</code> elements are positive integers in the range
     * <code>l...k-1</code>. <code>I</code> is array of size <code>n+1</code>,
     * used for temporary storage. <code>q</code> controls aggregation and
     * compaction by defining the maximum value for any symbol during
     * transformation: <code>q</code> must be at least <code>k-l</code>; if
     * <code>q<=n</code>, compaction is guaranteed; if <code>k-l>n</code>,
     * compaction is never done; if <code>q</code> is {@link Integer#MAX_VALUE}
     * , the maximum number of symbols are aggregated into one.
     * <p>
     * 
     * @return an integer <code>j</code> in the range <code>1...q</code>
     *         representing the size of the new alphabet. If <code>j<=n+1</code>
     *         , the alphabet is compacted. The global variable <code>r</code>
     *         is set to the number of old symbols grouped into one. Only
     *         <code>V[n]</code> is <code>0</code>.
     */
    private int transform(int n, int k, int l, int q) {
        int b, c, d, e, i, j, m, s;
        int pi, pj;// pointers

        for (s = 0, i = k - l; i != 0; i >>= 1)
            ++s; /* s is number of bits in old symbol. */
        e = Integer.MAX_VALUE >> s; /* e is for overflow checking. */
        for (b = d = r = 0; r < n && d <= e && (c = d << s | (k - l)) <= q; ++r) {
            b = b << s | (V[start + r] - l + 1); /*
                                                  * b is start of x in chunk
                                                  * alphabet.
                                                  */
            d = c; /* d is max symbol in chunk alphabet. */
        }
        m = (1 << (r - 1) * s) - 1; /* m masks off top old symbol from chunk. */
        V[start + n] = l - 1; /* emulate zero terminator. */
        if (d <= n) { /* if bucketing possible, compact alphabet. */
            for (pi = 0; pi <= d; ++pi)
                I[pi] = 0; /* zero transformation table. */
            for (pi = r, c = b; pi <= n; ++pi) {
                I[c] = 1; /* mark used chunk symbol. */
                c = (c & m) << s | (V[start + pi] - l + 1); /*
                                                             * shift in next old
                                                             * symbol in chunk.
                                                             */
            }
            for (i = 1; i < r; ++i) { /* handle last r-1 positions. */
                I[c] = 1; /* mark used chunk symbol. */
                c = (c & m) << s; /* shift in next old symbol in chunk. */
            }
            for (pi = 0, j = 1; pi <= d; ++pi)
                if (I[pi] != 0)
                    I[pi] = j++; /* j is new alphabet size. */
            for (pi = 0, pj = r, c = b; pj <= n; ++pi, ++pj) {
                V[start + pi] = I[c]; /* transform to new alphabet. */
                c = (c & m) << s | (V[start + pj] - l + 1); /*
                                                             * shift in next old
                                                             * symbol in chunk.
                                                             */
            }
            while (pi < n) { /* handle last r-1 positions. */
                V[start + pi++] = I[c]; /* transform to new alphabet. */
                c = (c & m) << s; /* shift right-end zero in chunk. */
            }
        } else { /* bucketing not possible, don't compact. */
            for (pi = 0, pj = r, c = b; pj <= n; ++pi, ++pj) {
                V[start + pi] = c; /* transform to new alphabet. */
                c = (c & m) << s | (V[start + pj] - l + 1); /*
                                                             * shift in next old
                                                             * symbol in chunk.
                                                             */
            }
            while (pi < n) { /* handle last r-1 positions. */
                V[start + pi++] = c; /* transform to new alphabet. */
                c = (c & m) << s; /* shift right-end zero in chunk. */
            }
            j = d + 1; /* new alphabet size. */
        }
        V[start + n] = 0; /* end-of-string symbol is zero. */
        return j; /* return new alphabet size. */
    }

    private int KEY(int p) {
        return V[start + I[p] + h];
    }

    private void SWAP(int a, int b) {
        int tmp = I[a];
        I[a] = I[b];
        I[b] = tmp;
    }

    private int MED3(int a, int b, int c) {
        return (KEY(a) < KEY(b) ? (KEY(b) < KEY(c) ? (b)
                : KEY(a) < KEY(c) ? (c) : (a)) : (KEY(b) > KEY(c) ? (b)
                : KEY(a) > KEY(c) ? (c) : (a)));
    }
}
