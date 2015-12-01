package edu.berkeley.cs.succinct.util.suffixarray;

import edu.berkeley.cs.succinct.util.container.BasicArray;
import edu.berkeley.cs.succinct.util.container.ByteArray;
import edu.berkeley.cs.succinct.util.container.CharArray;
import edu.berkeley.cs.succinct.util.container.IntArray;

/**
 * SA-IS algorithm, ported from Yuta Mori's implementation.
 *
 * Ge Nong, Sen Zhang and Wai Hong Chan, Two Efficient Algorithms for Linear Suffix Array
 * Construction, 2008.
 *
 * @see "http://yuta.256.googlepages.com/sais"
 */
public class SAIS {

  /* find the start or end of each bucket */
  private static void getCounts(BasicArray T, BasicArray C, int n, int k) {
    for (int i = 0; i < k; ++i) {
      C.set(i, 0);
    }
    for (int i = 0; i < n; ++i) {
      C.update(T.get(i), 1);
    }
  }

  private static void getBuckets(BasicArray C, BasicArray B, int k, boolean end) {
    int i, sum = 0;
    if (end) {
      for (i = 0; i < k; ++i) {
        sum += C.get(i);
        B.set(i, sum);
      }
    } else {
      for (i = 0; i < k; ++i) {
        sum += C.get(i);
        B.set(i, sum - C.get(i));
      }
    }
  }

  /* compute SA and BWT */
  private static void induceSA(BasicArray T, IntArray SA, BasicArray C, BasicArray B, int n, int k) {
    int b, i, j;
    int c0, c1;
    /* compute SAl */
    if (C == B) {
      getCounts(T, C, n, k);
    }
    getBuckets(C, B, k, false); /* find starts of buckets */
    j = n - 1;
    b = B.get(c1 = T.get(j));
    //SA[b++] = ((0 < j) && (T.get(j - 1) < c1)) ? ~j : j;
    SA.set(b++, ((0 < j) && (T.get(j - 1) < c1)) ? ~j : j);
    for (i = 0; i < n; ++i) {
      // j = SA[i];
      // SA[i] = ~j;
      j = SA.get(i);
      SA.set(i, ~j);
      if (0 < j) {
        if ((c0 = T.get(--j)) != c1) {
          B.set(c1, b);
          b = B.get(c1 = c0);
        }
        // SA[b++] = ((0 < j) && (T.get(j - 1) < c1)) ? ~j : j;
        SA.set(b++, ((0 < j) && (T.get(j - 1) < c1)) ? ~j : j);
      }
    }
    /* compute SAs */
    if (C == B) {
      getCounts(T, C, n, k);
    }
    getBuckets(C, B, k, true); /* find ends of buckets */
    for (i = n - 1, b = B.get(c1 = 0); 0 <= i; --i) {
      // if (0 < (j = SA[i])) {
      if (0 < (j = SA.get(i))) {
        if ((c0 = T.get(--j)) != c1) {
          B.set(c1, b);
          b = B.get(c1 = c0);
        }
        // SA[--b] = ((j == 0) || (T.get(j - 1) > c1)) ? ~j : j;
        SA.set(--b, ((j == 0) || (T.get(j - 1) > c1)) ? ~j : j);
      } else {
        // SA[i] = ~j;
        SA.set(i, ~j);
      }
    }
  }

  private static int computeBWT(BasicArray T, IntArray SA, BasicArray C, BasicArray B, int n, int k) {
    int b, i, j, pidx = -1;
    int c0, c1;
    /* compute SAl */
    if (C == B) {
      getCounts(T, C, n, k);
    }
    getBuckets(C, B, k, false); /* find starts of buckets */
    j = n - 1;
    b = B.get(c1 = T.get(j));
    // SA[b++] = ((0 < j) && (T.get(j - 1) < c1)) ? ~j : j;
    SA.set(b++, ((0 < j) && (T.get(j - 1) < c1)) ? ~j : j);
    for (i = 0; i < n; ++i) {
      // if (0 < (j = SA[i])) {
      if (0 < (j = SA.get(i))) {
        // SA[i] = ~(c0 = T.get(--j));
        SA.set(i, ~(c0 = T.get(--j)));
        if (c0 != c1) {
          B.set(c1, b);
          b = B.get(c1 = c0);
        }
        // SA[b++] = ((0 < j) && (T.get(j - 1) < c1)) ? ~j : j;
        SA.set(b++, ((0 < j) && (T.get(j - 1) < c1)) ? ~j : j);
      } else if (j != 0) {
        // SA[i] = ~j;
        SA.set(i, ~j);
      }
    }
    /* compute SAs */
    if (C == B) {
      getCounts(T, C, n, k);
    }
    getBuckets(C, B, k, true); /* find ends of buckets */
    for (i = n - 1, b = B.get(c1 = 0); 0 <= i; --i) {
      // if (0 < (j = SA[i])) {
      if (0 < (j = SA.get(i))) {
        // SA[i] = (c0 = T.get(--j));
        SA.set(i, (c0 = T.get(--j)));
        if (c0 != c1) {
          B.set(c1, b);
          b = B.get(c1 = c0);
        }
        // SA[--b] = ((0 < j) && (T.get(j - 1) > c1)) ? ~T.get(j - 1) : j;
        SA.set(--b, ((0 < j) && (T.get(j - 1) > c1)) ? ~T.get(j - 1) : j);
      } else if (j != 0) {
        // SA[i] = ~j;
        SA.set(i, ~j);
      } else {
        pidx = i;
      }
    }
    return pidx;
  }


  /* find the suffix array SA of T[0..n-1] in {0..k-1}^n
     use a working space (excluding T and SA) of at most 2n+O(1) for a constant alphabet */
  private static int SA_IS(BasicArray T, IntArray SA, int fs, int n, int k, boolean isbwt) {
    BasicArray C, B, RA;
    int i, j, c, m, p, q, plen, qlen, name, pidx = 0;
    int c0, c1;
    boolean diff;

    /* stage 1: reduce the problem by at least 1/2
       sort all the S-sub-strings */
    if (k <= fs) {
      C = new IntArray(SA, n);
      B = (k <= (fs - k)) ? new IntArray(SA, n + k) : C;
    } else {
      B = C = new IntArray(k);
    }
    getCounts(T, C, n, k);
    getBuckets(C, B, k, true); /* find ends of buckets */
    for (i = 0; i < n; ++i) {
      // SA[i] = 0;
      SA.set(i, 0);
    }
    for (i = n - 2, c = 0, c1 = T.get(n - 1); 0 <= i; --i, c1 = c0) {
      if ((c0 = T.get(i)) < (c1 + c)) {
        c = 1;
      } else if (c != 0) {
        SA.set(B.update(c1, -1), i + 1);
        // SA[B.update(c1, -1)] = i + 1;
        c = 0;
      }
    }
    induceSA(T, SA, C, B, n, k);
    C.destroy();
    B.destroy();

    /* compact all the sorted sub-strings into the first m items of SA
       2*m must be not larger than n (provable) */
    for (i = 0, m = 0; i < n; ++i) {
      p = SA.get(i);
      if ((0 < p) && (T.get(p - 1) > (c0 = T.get(p)))) {
        j = p + 1;
        while ((j < n) && (c0 == (c1 = T.get(j)))) {
          ++j;
        }
        if ((j < n) && (c0 < c1)) {
          // SA[m++] = p;
          SA.set(m++, p);
        }
      }
    }
    j = m + (n >> 1);
    for (i = m; i < j; ++i) {
      // SA[i] = 0;
      SA.set(i, 0);
    } /* init the name array buffer */
    /* store the length of all sub-strings */
    for (i = n - 2, j = n, c = 0, c1 = T.get(n - 1); 0 <= i; --i, c1 = c0) {
      if ((c0 = T.get(i)) < (c1 + c)) {
        c = 1;
      } else if (c != 0) {
        // SA[m + ((i + 1) >> 1)] = j - i - 1;
        SA.set(m + ((i + 1) >> 1), j - i - 1);
        j = i + 1;
        c = 0;
      }
    }
    /* find the lexicographic names of all sub-strings */
    for (i = 0, name = 0, q = n, qlen = 0; i < m; ++i) {
      // p = SA[i];
      // plen = SA[m + (p >> 1)];
      p = SA.get(i);
      plen = SA.get(m + (p >> 1));
      diff = true;
      if (plen == qlen) {
        j = 0;
        while ((j < plen) && (T.get(p + j) == T.get(q + j))) {
          ++j;
        }
        if (j == plen) {
          diff = false;
        }
      }
      if (diff) {
        ++name;
        q = p;
        qlen = plen;
      }
      // SA[m + (p >> 1)] = name;
      SA.set(m + (p >> 1), name);
    }

    /* stage 2: solve the reduced problem
       recurse if names are not yet unique */
    if (name < m) {
      RA = new IntArray(SA, n + fs - m);
      for (i = m + (n >> 1) - 1, j = n + fs - 1; m <= i; --i) {
        // if (SA[i] != 0) {
        //   SA[j--] = SA[i] - 1;
        if (SA.get(i) != 0) {
          SA.set(j--, SA.get(i) - 1);
        }
      }
      SA_IS(RA, SA, fs + n - m * 2, m, name, false);
      RA.destroy();
      for (i = n - 2, j = m * 2 - 1, c = 0, c1 = T.get(n - 1); 0 <= i; --i, c1 = c0) {
        if ((c0 = T.get(i)) < (c1 + c)) {
          c = 1;
        } else if (c != 0) {
          // SA[j--] = i + 1;
          SA.set(j--, i + 1);
          c = 0;
        } /* get p1 */
      }
      for (i = 0; i < m; ++i) {
        // SA[i] = SA[SA[i] + m];
        SA.set(i, SA.get(SA.get(i) + m));
      } /* get index */
    }

    /* stage 3: induce the result for the original problem */
    if (k <= fs) {
      C = new IntArray(SA, n);
      B = (k <= (fs - k)) ? new IntArray(SA, n + k) : C;
    } else {
      B = C = new IntArray(k);
    }
    /* put all left-most S characters into their buckets */
    getCounts(T, C, n, k);
    getBuckets(C, B, k, true); /* find ends of buckets */
    for (i = m; i < n; ++i) {
      // SA[i] = 0;
      SA.set(i, 0);
    } /* init SA[m..n-1] */
    for (i = m - 1; 0 <= i; --i) {
      // j = SA[i];
      // SA[i] = 0;
      // SA[B.update(T.get(j), -1)] = j;
      j = SA.get(i);
      SA.set(i, 0);
      SA.set(B.update(T.get(j), -1), j);
    }
    if (!isbwt) {
      induceSA(T, SA, C, B, n, k);
    } else {
      pidx = computeBWT(T, SA, C, B, n, k);
    }
    C.destroy();
    B.destroy();

    return pidx;
  }


  /**
   * Suffix sorting
   **/

  /* byte */
  private static int suffixSort(byte[] T, IntArray SA, int n) {
    if ((T == null) || (SA == null) || (T.length < n) || (SA.length() < n)) {
      return -1;
    }
    if (n <= 1) {
      if (n == 1) {
        // SA[0] = 0;
        SA.set(0, 0);
      }
      return 0;
    }
    return SA_IS(new ByteArray(T), SA, 0, n, 256, false);
  }

  /* char */
  private static int suffixSort(char[] T, IntArray SA, int n) {
    if ((T == null) || (SA == null) || (T.length < n) || (SA.length() < n)) {
      return -1;
    }
    if (n <= 1) {
      if (n == 1) {
        // SA[0] = 0;
        SA.set(0, 0);
      }
      return 0;
    }
    return SA_IS(new CharArray(T), SA, 0, n, 65536, false);
  }

  public static IntArray buildSuffixArray(char[] input) {
    final IntArray SA = new IntArray(input.length);
    suffixSort(input, SA, input.length);
    return SA;
  }

  public static IntArray buildSuffixArray(byte[] input) {
    final IntArray SA = new IntArray(input.length);
    suffixSort(input, SA, input.length);
    return SA;
  }
}
