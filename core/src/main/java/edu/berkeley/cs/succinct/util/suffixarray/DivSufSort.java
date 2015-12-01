package edu.berkeley.cs.succinct.util.suffixarray;

/**
 * <p/>
 * Straightforward reimplementation of the divsufsort algorithm given in: <pre><code>
 * Yuta Mori, Short description of improved two-stage suffix sorting
 * algorithm, 2005.
 * http://homepage3.nifty.com/wpage/software/itssort.txt
 * </code></pre>
 * <p/>
 * This implementation is basically a translation of the C version given by Yuta Mori:
 * <tt>libdivsufsort-2.0.0, http://code.google.com/p/libdivsufsort/</tt>
 * <p/>
 * The implementation of this algorithm makes some assumptions about the input. See
 * {@link #buildSuffixArray(byte[])} for details.
 */
public final class DivSufSort {
  /*
   *
   */
  private final static class StackElement {
    final int a, b, c, e;
    int d;

    StackElement(int a, int b, int c, int d, int e) {
      this.a = a;
      this.b = b;
      this.c = c;
      this.d = d;
      this.e = e;
    }

    StackElement(int a, int b, int c, int d) {
      this(a, b, c, d, 0);
    }
  }


  /*
   *
   */
  private final static class TRBudget {
    int chance;
    int remain;
    int incval;
    int count;

    private TRBudget(int chance, int incval) {
      this.chance = chance;
      this.remain = incval;
      this.incval = incval;
    }

    private int check(int size) {
      if (size <= this.remain) {
        this.remain -= size;
        return 1;
      }
      if (this.chance == 0) {
        this.count += size;
        return 0;
      }
      this.remain += this.incval - size;
      this.chance -= 1;
      return 1;
    }
  }


  /*
   *
   */
  private static final class TRPartitionResult {
    final int a;
    final int b;

    public TRPartitionResult(int a, int b) {
      this.a = a;
      this.b = b;
    }
  }

  /**
   * @param alphabetSize The size of the input alphabet.
   */
  public DivSufSort(int alphabetSize) {
    ALPHABET_SIZE = alphabetSize;
    BUCKET_A_SIZE = ALPHABET_SIZE;
    BUCKET_B_SIZE = ALPHABET_SIZE * ALPHABET_SIZE;
  }

  public DivSufSort() {
    this(DEFAULT_ALPHABET_SIZE);
  }

    /* constants */

  private final static int DEFAULT_ALPHABET_SIZE = 256;
  private final static int SS_INSERTIONSORT_THRESHOLD = 8;
  private final static int SS_BLOCKSIZE = 1024;
  private final static int SS_MISORT_STACKSIZE = 16;
  private final static int SS_SMERGE_STACKSIZE = 32;
  private final static int TR_STACKSIZE = 64;
  private final static int TR_INSERTIONSORT_THRESHOLD = 8;

  private final static int[] sqq_table =
    {0, 16, 22, 27, 32, 35, 39, 42, 45, 48, 50, 53, 55, 57, 59, 61, 64, 65, 67, 69, 71, 73, 75, 76,
      78, 80, 81, 83, 84, 86, 87, 89, 90, 91, 93, 94, 96, 97, 98, 99, 101, 102, 103, 104, 106, 107,
      108, 109, 110, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 128,
      128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 144, 145,
      146, 147, 148, 149, 150, 150, 151, 152, 153, 154, 155, 155, 156, 157, 158, 159, 160, 160, 161,
      162, 163, 163, 164, 165, 166, 167, 167, 168, 169, 170, 170, 171, 172, 173, 173, 174, 175, 176,
      176, 177, 178, 178, 179, 180, 181, 181, 182, 183, 183, 184, 185, 185, 186, 187, 187, 188, 189,
      189, 190, 191, 192, 192, 193, 193, 194, 195, 195, 196, 197, 197, 198, 199, 199, 200, 201, 201,
      202, 203, 203, 204, 204, 205, 206, 206, 207, 208, 208, 209, 209, 210, 211, 211, 212, 212, 213,
      214, 214, 215, 215, 216, 217, 217, 218, 218, 219, 219, 220, 221, 221, 222, 222, 223, 224, 224,
      225, 225, 226, 226, 227, 227, 228, 229, 229, 230, 230, 231, 231, 232, 232, 233, 234, 234, 235,
      235, 236, 236, 237, 237, 238, 238, 239, 240, 240, 241, 241, 242, 242, 243, 243, 244, 244, 245,
      245, 246, 246, 247, 247, 248, 248, 249, 249, 250, 250, 251, 251, 252, 252, 253, 253, 254, 254,
      255};

  private final static int[] lg_table =
    {-1, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
      4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
      5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
      6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
      6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
      7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
      7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
      7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
      7, 7, 7, 7, 7, 7, 7, 7};

  /* fields */
  private final int ALPHABET_SIZE;
  private final int BUCKET_A_SIZE;
  private final int BUCKET_B_SIZE;
  private int[] SA;
  private byte[] T;

  public final void buildSuffixArray(byte[] input) {
    assert input != null;
    assert input.length >= 2;

    this.SA = new int[input.length];
    this.T = input;
    int[] bucket_A = new int[BUCKET_A_SIZE];
    int[] bucket_B = new int[BUCKET_B_SIZE];
    /* Suffix sort. */
    int m = sortTypeBstar(bucket_A, bucket_B, input.length);
    constructSuffixArray(bucket_A, bucket_B, input.length, m);
  }

  public int[] getSA() {
    return SA;
  }

  /**
   * Constructs the suffix array by using the sorted order of type B* suffixes.
   */
  private void constructSuffixArray(int[] bucket_A, int[] bucket_B, int n, int m) {
    int i, j, k; // ptr
    int s, c0, c1, c2;
    // (_c1)])
    if (0 < m) {
      /*
       * Construct the sorted order of type B suffixes by using the sorted order of
       * type B suffixes.
       */
      for (c1 = ALPHABET_SIZE - 2; 0 <= c1; --c1) {
        /* Scan the suffix array from right to left. */
        for (
          i = bucket_B[(c1) * ALPHABET_SIZE + (c1 + 1)], j = bucket_A[c1 + 1] - 1, k = 0, c2 = -1;
          i <= j; --j) {
          if (0 < (s = SA[j])) {
            // Tools.assertAlways(T[s] == c1, "");
            // Tools.assertAlways(((s + 1) < n) && (T[s] <= T[s +
            // 1]),
            // "");
            // Tools.assertAlways(T[s - 1] <= T[s], "");
            SA[j] = ~s;
            c0 = T[--s];
            if ((0 < s) && (T[s - 1] > c0)) {
              s = ~s;
            }
            if (c0 != c2) {
              if (0 <= c2) {
                bucket_B[(c1) * ALPHABET_SIZE + (c2)] = k;
              }
              k = bucket_B[(c1) * ALPHABET_SIZE + (c2 = c0)];
            }
            // Tools.assertAlways(k < j, "");
            SA[k--] = s;
          } else {
            // Tools.assertAlways(((s == 0) && (T[s] == c1))
            // || (s < 0), "");
            SA[j] = ~s;
          }
        }
      }
    }

    /*
     * Construct the suffix array by using the sorted order of type B suffixes.
     */
    k = bucket_A[c2 = T[n - 1]];
    SA[k++] = (T[n - 2] < c2) ? ~(n - 1) : (n - 1);
    /* Scan the suffix array from left to right. */
    for (i = 0, j = n; i < j; ++i) {
      if (0 < (s = SA[i])) {
        // Tools.assertAlways(T[s - 1] >= T[s], "");
        c0 = T[--s];
        if ((s == 0) || (T[s - 1] < c0)) {
          s = ~s;
        }
        if (c0 != c2) {
          bucket_A[c2] = k;
          k = bucket_A[c2 = c0];
        }
        // Tools.assertAlways(i < k, "");
        SA[k++] = s;
      } else {
        // Tools.assertAlways(s < 0, "");
        SA[i] = ~s;
      }
    }
  }

  /**
   *
   */
  private int sortTypeBstar(int[] bucket_A, int[] bucket_B, int n) {
    int PAb, ISAb, buf;

    int i, j, k, t, m, bufsize;
    int c0, c1;

    /*
     * Count the number of occurrences of the first one or two characters of each type
     * A, B and B suffix. Moreover, store the beginning position of all type B
     * suffixes into the array SA.
     */
    for (i = n - 1, m = n, c0 = T[n - 1]; 0 <= i; ) {
            /* type A suffix. */
      do {
        ++bucket_A[c1 = c0];
      } while ((0 <= --i) && ((c0 = T[i]) >= c1));
      if (0 <= i) {
                /* type B suffix. */
        ++bucket_B[(c0) * ALPHABET_SIZE + (c1)];
        SA[--m] = i;
                /* type B suffix. */
        for (--i, c1 = c0; (0 <= i) && ((c0 = T[i]) <= c1); --i, c1 = c0) {
          ++bucket_B[(c1) * ALPHABET_SIZE + (c0)];
        }
      }
    }
    m = n - m;

    // note:
    // A type B* suffix is lexicographically smaller than a type B suffix
    // that
    // begins with the same first two characters.

    // Calculate the index of start/end point of each bucket.
    for (c0 = 0, i = 0, j = 0; c0 < ALPHABET_SIZE; ++c0) {
      t = i + bucket_A[c0];
      bucket_A[c0] = i + j; /* start point */
      i = t + bucket_B[(c0) * ALPHABET_SIZE + (c0)];
      for (c1 = c0 + 1; c1 < ALPHABET_SIZE; ++c1) {
        j += bucket_B[(c0) * ALPHABET_SIZE + (c1)];
        bucket_B[(c0) * ALPHABET_SIZE + (c1)] = j; // end point
        i += bucket_B[(c1) * ALPHABET_SIZE + (c0)];
      }
    }

    if (0 < m) {
      // Sort the type B* suffixes by their first two characters.
      PAb = n - m;// SA
      ISAb = m;// SA
      for (i = m - 2; 0 <= i; --i) {
        t = SA[PAb + i];
        c0 = T[t];
        c1 = T[t + 1];
        SA[--bucket_B[(c0) * ALPHABET_SIZE + (c1)]] = i;
      }
      t = SA[PAb + m - 1];
      c0 = T[t];
      c1 = T[t + 1];
      SA[--bucket_B[(c0) * ALPHABET_SIZE + (c1)]] = m - 1;

      // Sort the type B* substrings using sssort.

      buf = m;// SA
      bufsize = n - (2 * m);

      for (c0 = ALPHABET_SIZE - 2, j = m; 0 < j; --c0) {
        for (c1 = ALPHABET_SIZE - 1; c0 < c1; j = i, --c1) {
          i = bucket_B[(c0) * ALPHABET_SIZE + (c1)];
          if (1 < (j - i)) {
            ssSort(PAb, i, j, buf, bufsize, 2, n, SA[i] == (m - 1));
          }
        }
      }

      // Compute ranks of type B* substrings.
      for (i = m - 1; 0 <= i; --i) {
        if (0 <= SA[i]) {
          j = i;
          do {
            SA[ISAb + SA[i]] = i;
          } while ((0 <= --i) && (0 <= SA[i]));
          SA[i + 1] = i - j;
          if (i <= 0) {
            break;
          }
        }
        j = i;
        do {
          SA[ISAb + (SA[i] = ~SA[i])] = j;
        } while (SA[--i] < 0);
        SA[ISAb + SA[i]] = j;
      }
      // Construct the inverse suffix array of type B* suffixes using
      // trsort.
      trSort(ISAb, m, 1);
      // Set the sorted order of type B* suffixes.
      for (i = n - 1, j = m, c0 = T[n - 1]; 0 <= i; ) {
        for (--i, c1 = c0; (0 <= i) && ((c0 = T[i]) >= c1); --i, c1 = c0) {
        }
        if (0 <= i) {
          t = i;
          for (--i, c1 = c0; (0 <= i) && ((c0 = T[i]) <= c1); --i, c1 = c0) {
          }
          SA[SA[ISAb + --j]] = ((t == 0) || (1 < (t - i))) ? t : ~t;
        }
      }

      // Calculate the index of start/end point of each bucket.
      bucket_B[(ALPHABET_SIZE - 1) * ALPHABET_SIZE + (ALPHABET_SIZE - 1)] = n; // end
      // point
      for (c0 = ALPHABET_SIZE - 2, k = m - 1; 0 <= c0; --c0) {
        i = bucket_A[c0 + 1] - 1;
        for (c1 = ALPHABET_SIZE - 1; c0 < c1; --c1) {
          t = i - bucket_B[(c1) * ALPHABET_SIZE + (c0)];
          bucket_B[(c1) * ALPHABET_SIZE + (c0)] = i; // end point

          // Move all type B* suffixes to the correct position.
          for (i = t, j = bucket_B[(c0) * ALPHABET_SIZE + (c1)]; j <= k; --i, --k) {
            SA[i] = SA[k];
          }
        }
        bucket_B[(c0) * ALPHABET_SIZE + (c0 + 1)] =
          i - bucket_B[(c0) * ALPHABET_SIZE + (c0)] + 1; //
        bucket_B[(c0) * ALPHABET_SIZE + (c0)] = i; // end point
      }
    }

    return m;
  }

  /**
   *
   */
  private void ssSort(final int PA, int first, int last, int buf, int bufsize, int depth,
    int n, boolean lastsuffix) {
    int a, b, middle, curbuf;// SA pointer

    int j, k, curbufsize, limit;

    int i;

    if (lastsuffix) {
      ++first;
    }

    if ((bufsize < SS_BLOCKSIZE) && (bufsize < (last - first)) && (bufsize < (limit =
      ssIsqrt(last - first)))) {
      if (SS_BLOCKSIZE < limit) {
        limit = SS_BLOCKSIZE;
      }
      buf = middle = last - limit;
      bufsize = limit;
    } else {
      middle = last;
      limit = 0;
    }
    for (a = first, i = 0; SS_BLOCKSIZE < (middle - a); a += SS_BLOCKSIZE, ++i) {
      ssMintroSort(PA, a, a + SS_BLOCKSIZE, depth);
      curbufsize = last - (a + SS_BLOCKSIZE);
      curbuf = a + SS_BLOCKSIZE;
      if (curbufsize <= bufsize) {
        curbufsize = bufsize;
        curbuf = buf;
      }
      for (b = a, k = SS_BLOCKSIZE, j = i; (j & 1) != 0; b -= k, k <<= 1, j >>= 1) {
        ssSwapMerge(PA, b - k, b, b + k, curbuf, curbufsize, depth);
      }
    }
    ssMintroSort(PA, a, middle, depth);
    for (k = SS_BLOCKSIZE; i != 0; k <<= 1, i >>= 1) {
      if ((i & 1) != 0) {
        ssSwapMerge(PA, a - k, a, middle, buf, bufsize, depth);
        a -= k;
      }
    }
    if (limit != 0) {
      ssMintroSort(PA, middle, last, depth);
      ssInplaceMerge(PA, first, middle, last, depth);
    }

    if (lastsuffix) {
      int p1 = SA[PA + SA[first - 1]];
      int p11 = n - 2;
      for (
        a = first, i = SA[first - 1];
        (a < last) && ((SA[a] < 0) || (0 < ssCompare(p1, p11, PA + SA[a], depth))); ++a) {
        SA[a - 1] = SA[a];
      }
      SA[a - 1] = i;
    }

  }

  /**
   * special version of ss_compare for handling
   * <code>ss_compare(T, &(PAi[0]), PA + *a, depth)</code> situation.
   */
  private int ssCompare(int pa, int pb, int p2, int depth) {
    int U1, U2, U1n, U2n;// pointers to T

    for (
      U1 = depth + pa, U2 = depth + SA[p2], U1n = pb + 2, U2n = SA[p2 + 1] + 2;
      (U1 < U1n) && (U2 < U2n) && (T[U1] == T[U2]); ++U1, ++U2) {
    }

    return U1 < U1n ? (U2 < U2n ? T[U1] - T[U2] : 1) : (U2 < U2n ? -1 : 0);
  }

  /**
   *
   */
  private int ssCompare(int p1, int p2, int depth) {
    int U1, U2, U1n, U2n;// pointers to T

    for (
      U1 = depth + SA[p1], U2 = depth + SA[p2], U1n = SA[p1 + 1] + 2, U2n = SA[p2 + 1] + 2;
      (U1 < U1n) && (U2 < U2n) && (T[U1] == T[U2]); ++U1, ++U2) {
    }

    return U1 < U1n ? (U2 < U2n ? T[U1] - T[U2] : 1) : (U2 < U2n ? -1 : 0);

  }

  /**
   *
   */
  private void ssInplaceMerge(int PA, int first, int middle, int last, int depth) {
    // PA, middle, first, last are pointers to SA
    int p, a, b;// pointer to SA
    int len, half;
    int q, r;
    int x;

    for (; ; ) {
      if (SA[last - 1] < 0) {
        x = 1;
        p = PA + ~SA[last - 1];
      } else {
        x = 0;
        p = PA + SA[last - 1];
      }
      for (
        a = first, len = middle - first, half = len >> 1, r = -1; 0 < len; len = half, half >>= 1) {
        b = a + half;
        q = ssCompare(PA + ((0 <= SA[b]) ? SA[b] : ~SA[b]), p, depth);
        if (q < 0) {
          a = b + 1;
          half -= (len & 1) ^ 1;
        } else {
          r = q;
        }
      }
      if (a < middle) {
        if (r == 0) {
          SA[a] = ~SA[a];
        }
        ssRotate(a, middle, last);
        last -= middle - a;
        middle = a;
        if (first == middle) {
          break;
        }
      }
      --last;
      if (x != 0) {
        while (SA[--last] < 0) {
          // nop
        }
      }
      if (middle == last) {
        break;
      }
    }

  }

  /**
   *
   */
  private void ssRotate(int first, int middle, int last) {
    // first, middle, last are pointers in SA
    int a, b, t;// pointers in SA
    int l, r;
    l = middle - first;
    r = last - middle;
    for (; (0 < l) && (0 < r); ) {
      if (l == r) {
        ssBlockSwap(first, middle, l);
        break;
      }
      if (l < r) {
        a = last - 1;
        b = middle - 1;
        t = SA[a];
        do {
          SA[a--] = SA[b];
          SA[b--] = SA[a];
          if (b < first) {
            SA[a] = t;
            last = a;
            if ((r -= l + 1) <= l) {
              break;
            }
            a -= 1;
            b = middle - 1;
            t = SA[a];
          }
        } while (true);
      } else {
        a = first;
        b = middle;
        t = SA[a];
        do {
          SA[a++] = SA[b];
          SA[b++] = SA[a];
          if (last <= b) {
            SA[a] = t;
            first = a + 1;
            if ((l -= r + 1) <= r) {
              break;
            }
            a += 1;
            b = middle;
            t = SA[a];
          }
        } while (true);
      }
    }
  }

  /**
   *
   */
  private void ssBlockSwap(int a, int b, int n) {
    // a, b -- pointer to SA
    int t;
    for (; 0 < n; --n, ++a, ++b) {
      t = SA[a];
      SA[a] = SA[b];
      SA[b] = t;
    }
  }

  private static int getIDX(int a) {
    return (0 <= (a)) ? (a) : (~(a));
  }

  private static int min(int a, int b) {
    return a < b ? a : b;
  }

  /**
   * D&C based merge.
   */
  private void ssSwapMerge(int PA, int first, int middle, int last, int buf, int bufsize,
    int depth) {
    // Pa, first, middle, last and buf - pointers in SA array

    final int STACK_SIZE = SS_SMERGE_STACKSIZE;
    StackElement[] stack = new StackElement[STACK_SIZE];
    int l, r, lm, rm;// pointers in SA
    int m, len, half;
    int ssize;
    int check, next;

    for (check = 0, ssize = 0; ; ) {

      if ((last - middle) <= bufsize) {
        if ((first < middle) && (middle < last)) {
          ssMergeBackward(PA, first, middle, last, buf, depth);
        }
        if (((check & 1) != 0) || (((check & 2) != 0) && (
          ssCompare(PA + getIDX(SA[first - 1]), PA + SA[first], depth) == 0))) {
          SA[first] = ~SA[first];
        }
        if (((check & 4) != 0) && ((ssCompare(PA + getIDX(SA[last - 1]), PA + SA[last], depth)
          == 0))) {
          SA[last] = ~SA[last];
        }

        if (ssize > 0) {
          StackElement se = stack[--ssize];
          first = se.a;
          middle = se.b;
          last = se.c;
          check = se.d;
        } else {
          return;
        }
        continue;
      }

      if ((middle - first) <= bufsize) {
        if (first < middle) {
          ssMergeForward(PA, first, middle, last, buf, depth);
        }
        if (((check & 1) != 0) || (((check & 2) != 0) && (
          ssCompare(PA + getIDX(SA[first - 1]), PA + SA[first], depth) == 0))) {
          SA[first] = ~SA[first];
        }
        if (((check & 4) != 0) && ((ssCompare(PA + getIDX(SA[last - 1]), PA + SA[last], depth)
          == 0))) {
          SA[last] = ~SA[last];
        }

        if (ssize > 0) {
          StackElement se = stack[--ssize];
          first = se.a;
          middle = se.b;
          last = se.c;
          check = se.d;
        } else {
          return;
        }

        continue;
      }

      for (
        m = 0, len = min(middle - first, last - middle), half = len >> 1;
        0 < len; len = half, half >>= 1) {
        if (ssCompare(PA + getIDX(SA[middle + m + half]), PA + getIDX(SA[middle - m - half - 1]),
          depth) < 0) {
          m += half + 1;
          half -= (len & 1) ^ 1;
        }
      }

      if (0 < m) {
        lm = middle - m;
        rm = middle + m;
        ssBlockSwap(lm, middle, m);
        l = r = middle;
        next = 0;
        if (rm < last) {
          if (SA[rm] < 0) {
            SA[rm] = ~SA[rm];
            if (first < lm) {
              for (; SA[--l] < 0; ) {
              }
              next |= 4;
            }
            next |= 1;
          } else if (first < lm) {
            for (; SA[r] < 0; ++r) {
            }
            next |= 2;
          }
        }

        if ((l - first) <= (last - r)) {
          stack[ssize++] = new StackElement(r, rm, last, (next & 3) | (check & 4));

          middle = lm;
          last = l;
          check = (check & 3) | (next & 4);
        } else {
          if (((next & 2) != 0) && (r == middle)) {
            next ^= 6;
          }
          stack[ssize++] = new StackElement(first, lm, l, (check & 3) | (next & 4));

          first = r;
          middle = rm;
          check = (next & 3) | (check & 4);
        }
      } else {
        if (ssCompare(PA + getIDX(SA[middle - 1]), PA + SA[middle], depth) == 0) {
          SA[middle] = ~SA[middle];
        }

        if (((check & 1) != 0) || (((check & 2) != 0) && (
          ssCompare(PA + getIDX(SA[first - 1]), PA + SA[first], depth) == 0))) {
          SA[first] = ~SA[first];
        }
        if (((check & 4) != 0) && ((ssCompare(PA + getIDX(SA[last - 1]), PA + SA[last], depth)
          == 0))) {
          SA[last] = ~SA[last];
        }

        if (ssize > 0) {
          StackElement se = stack[--ssize];
          first = se.a;
          middle = se.b;
          last = se.c;
          check = se.d;
        } else {
          return;
        }

      }

    }

  }

  /**
   * Merge-forward with internal buffer.
   */
  private void ssMergeForward(int PA, int first, int middle, int last, int buf, int depth) {
    // PA, first, middle, last, buf are pointers to SA
    int a, b, c, bufend;// pointers to SA
    int t, r;

    bufend = buf + (middle - first) - 1;
    ssBlockSwap(buf, first, middle - first);

    for (t = SA[a = first], b = buf, c = middle; ; ) {
      r = ssCompare(PA + SA[b], PA + SA[c], depth);
      if (r < 0) {
        do {
          SA[a++] = SA[b];
          if (bufend <= b) {
            SA[bufend] = t;
            return;
          }
          SA[b++] = SA[a];
        } while (SA[b] < 0);
      } else if (r > 0) {
        do {
          SA[a++] = SA[c];
          SA[c++] = SA[a];
          if (last <= c) {
            while (b < bufend) {
              SA[a++] = SA[b];
              SA[b++] = SA[a];
            }
            SA[a] = SA[b];
            SA[b] = t;
            return;
          }
        } while (SA[c] < 0);
      } else {
        SA[c] = ~SA[c];
        do {
          SA[a++] = SA[b];
          if (bufend <= b) {
            SA[bufend] = t;
            return;
          }
          SA[b++] = SA[a];
        } while (SA[b] < 0);

        do {
          SA[a++] = SA[c];
          SA[c++] = SA[a];
          if (last <= c) {
            while (b < bufend) {
              SA[a++] = SA[b];
              SA[b++] = SA[a];
            }
            SA[a] = SA[b];
            SA[b] = t;
            return;
          }
        } while (SA[c] < 0);
      }
    }

  }

  /**
   * Merge-backward with internal buffer.
   */
  private void ssMergeBackward(int PA, int first, int middle, int last, int buf, int depth) {
    // PA, first, middle, last, buf are pointers in SA
    int p1, p2;// pointers in SA
    int a, b, c, bufend;// pointers in SA
    int t, r, x;

    bufend = buf + (last - middle) - 1;
    ssBlockSwap(buf, middle, last - middle);

    x = 0;
    if (SA[bufend] < 0) {
      p1 = PA + ~SA[bufend];
      x |= 1;
    } else {
      p1 = PA + SA[bufend];
    }
    if (SA[middle - 1] < 0) {
      p2 = PA + ~SA[middle - 1];
      x |= 2;
    } else {
      p2 = PA + SA[middle - 1];
    }
    for (t = SA[a = last - 1], b = bufend, c = middle - 1; ; ) {
      r = ssCompare(p1, p2, depth);
      if (0 < r) {
        if ((x & 1) != 0) {
          do {
            SA[a--] = SA[b];
            SA[b--] = SA[a];
          } while (SA[b] < 0);
          x ^= 1;
        }
        SA[a--] = SA[b];
        if (b <= buf) {
          SA[buf] = t;
          break;
        }
        SA[b--] = SA[a];
        if (SA[b] < 0) {
          p1 = PA + ~SA[b];
          x |= 1;
        } else {
          p1 = PA + SA[b];
        }
      } else if (r < 0) {
        if ((x & 2) != 0) {
          do {
            SA[a--] = SA[c];
            SA[c--] = SA[a];
          } while (SA[c] < 0);
          x ^= 2;
        }
        SA[a--] = SA[c];
        SA[c--] = SA[a];
        if (c < first) {
          while (buf < b) {
            SA[a--] = SA[b];
            SA[b--] = SA[a];
          }
          SA[a] = SA[b];
          SA[b] = t;
          break;
        }
        if (SA[c] < 0) {
          p2 = PA + ~SA[c];
          x |= 2;
        } else {
          p2 = PA + SA[c];
        }
      } else {
        if ((x & 1) != 0) {
          do {
            SA[a--] = SA[b];
            SA[b--] = SA[a];
          } while (SA[b] < 0);
          x ^= 1;
        }
        SA[a--] = ~SA[b];
        if (b <= buf) {
          SA[buf] = t;
          break;
        }
        SA[b--] = SA[a];
        if ((x & 2) != 0) {
          do {
            SA[a--] = SA[c];
            SA[c--] = SA[a];
          } while (SA[c] < 0);
          x ^= 2;
        }
        SA[a--] = SA[c];
        SA[c--] = SA[a];
        if (c < first) {
          while (buf < b) {
            SA[a--] = SA[b];
            SA[b--] = SA[a];
          }
          SA[a] = SA[b];
          SA[b] = t;
          break;
        }
        if (SA[b] < 0) {
          p1 = PA + ~SA[b];
          x |= 1;
        } else {
          p1 = PA + SA[b];
        }
        if (SA[c] < 0) {
          p2 = PA + ~SA[c];
          x |= 2;
        } else {
          p2 = PA + SA[c];
        }
      }
    }
  }

  /**
   * Insertionsort for small size groups
   */
  private void ssInsertionSort(int PA, int first, int last, int depth) {
    // PA, first, last are pointers in SA
    int i, j;// pointers in SA
    int t, r;

    for (i = last - 2; first <= i; --i) {
      for (t = SA[i], j = i + 1; 0 < (r = ssCompare(PA + t, PA + SA[j], depth)); ) {
        do {
          SA[j - 1] = SA[j];
        } while ((++j < last) && (SA[j] < 0));
        if (last <= j) {
          break;
        }
      }
      if (r == 0) {
        SA[j] = ~SA[j];
      }
      SA[j - 1] = t;
    }

  }

  /**
   *
   */
  private static int ssIsqrt(int x) {
    int y, e;

    if (x >= (SS_BLOCKSIZE * SS_BLOCKSIZE)) {
      return SS_BLOCKSIZE;
    }
    e = ((x & 0xffff0000) != 0) ?
      (((x & 0xff000000) != 0) ?
        24 + lg_table[(x >> 24) & 0xff] :
        16 + lg_table[(x >> 16) & 0xff]) :
      (x & 0x0000ff00) != 0 ? 8 + lg_table[x >> 8 & 0xff] : lg_table[x & 0xff];

    if (e >= 16) {
      y = sqq_table[x >> ((e - 6) - (e & 1))] << ((e >> 1) - 7);
      if (e >= 24) {
        y = (y + 1 + x / y) >> 1;
      }
      y = (y + 1 + x / y) >> 1;
    } else if (e >= 8) {
      y = (sqq_table[x >> ((e - 6) - (e & 1))] >> (7 - (e >> 1))) + 1;
    } else {
      return sqq_table[x] >> 4;
    }

    return (x < (y * y)) ? y - 1 : y;
  }

  /* Multikey introsort for medium size groups. */
  private void ssMintroSort(int PA, int first, int last, int depth) {
    final int STACK_SIZE = SS_MISORT_STACKSIZE;
    StackElement[] stack = new StackElement[STACK_SIZE];
    int Td;// T ptr
    int a, b, c, d, e, f;// SA ptr
    int s, t;
    int ssize;
    int limit;
    int v, x = 0;
    for (ssize = 0, limit = ssIlg(last - first); ; ) {

      if ((last - first) <= SS_INSERTIONSORT_THRESHOLD) {
        if (1 < (last - first)) {
          ssInsertionSort(PA, first, last, depth);
        }
        if (ssize > 0) {
          StackElement se = stack[--ssize];
          first = se.a;
          last = se.b;
          depth = se.c;
          limit = se.d;
        } else {
          return;
        }

        continue;
      }

      Td = depth;
      if (limit-- == 0) {
        ssHeapSort(Td, PA, first, last - first);

      }
      if (limit < 0) {
        for (a = first + 1, v = T[Td + SA[PA + SA[first]]]; a < last; ++a) {
          if ((x = T[Td + SA[PA + SA[a]]]) != v) {
            if (1 < (a - first)) {
              break;
            }
            v = x;
            first = a;
          }
        }

        if (T[Td + SA[PA + SA[first]] - 1] < v) {
          first = ssPartition(PA, first, a, depth);
        }
        if ((a - first) <= (last - a)) {
          if (1 < (a - first)) {
            stack[ssize++] = new StackElement(a, last, depth, -1);
            last = a;
            depth += 1;
            limit = ssIlg(a - first);
          } else {
            first = a;
            limit = -1;
          }
        } else {
          if (1 < (last - a)) {
            stack[ssize++] = new StackElement(first, a, depth + 1, ssIlg(a - first));
            first = a;
            limit = -1;
          } else {
            last = a;
            depth += 1;
            limit = ssIlg(a - first);
          }
        }
        continue;
      }

      // choose pivot
      a = ssPivot(Td, PA, first, last);
      v = T[Td + SA[PA + SA[a]]];
      swapInSA(first, a);

      // partition
      for (b = first; (++b < last) && ((x = T[Td + SA[PA + SA[b]]]) == v); ) {
      }
      if (((a = b) < last) && (x < v)) {
        for (; (++b < last) && ((x = T[Td + SA[PA + SA[b]]]) <= v); ) {
          if (x == v) {
            swapInSA(b, a);
            ++a;
          }
        }
      }

      for (c = last; (b < --c) && ((x = T[Td + SA[PA + SA[c]]]) == v); ) {
      }
      if ((b < (d = c)) && (x > v)) {
        for (; (b < --c) && ((x = T[Td + SA[PA + SA[c]]]) >= v); ) {
          if (x == v) {
            swapInSA(c, d);
            --d;
          }
        }
      }

      for (; b < c; ) {
        swapInSA(b, c);
        for (; (++b < c) && ((x = T[Td + SA[PA + SA[b]]]) <= v); ) {
          if (x == v) {
            swapInSA(b, a);
            ++a;
          }
        }
        for (; (b < --c) && ((x = T[Td + SA[PA + SA[c]]]) >= v); ) {
          if (x == v) {
            swapInSA(c, d);
            --d;
          }
        }
      }

      if (a <= d) {
        c = b - 1;

        if ((s = a - first) > (t = b - a)) {
          s = t;
        }
        for (e = first, f = b - s; 0 < s; --s, ++e, ++f) {
          swapInSA(e, f);
        }
        if ((s = d - c) > (t = last - d - 1)) {
          s = t;
        }
        for (e = b, f = last - s; 0 < s; --s, ++e, ++f) {
          swapInSA(e, f);
        }

        a = first + (b - a);
        c = last - (d - c);
        b = (v <= T[Td + SA[PA + SA[a]] - 1]) ? a : ssPartition(PA, a, c, depth);

        if ((a - first) <= (last - c)) {
          if ((last - c) <= (c - b)) {
            stack[ssize++] = new StackElement(b, c, depth + 1, ssIlg(c - b));
            stack[ssize++] = new StackElement(c, last, depth, limit);
            last = a;
          } else if ((a - first) <= (c - b)) {
            stack[ssize++] = new StackElement(c, last, depth, limit);
            stack[ssize++] = new StackElement(b, c, depth + 1, ssIlg(c - b));
            last = a;
          } else {
            stack[ssize++] = new StackElement(c, last, depth, limit);
            stack[ssize++] = new StackElement(first, a, depth, limit);
            first = b;
            last = c;
            depth += 1;
            limit = ssIlg(c - b);
          }
        } else {
          if ((a - first) <= (c - b)) {
            stack[ssize++] = new StackElement(b, c, depth + 1, ssIlg(c - b));
            stack[ssize++] = new StackElement(first, a, depth, limit);
            first = c;
          } else if ((last - c) <= (c - b)) {
            stack[ssize++] = new StackElement(first, a, depth, limit);
            stack[ssize++] = new StackElement(b, c, depth + 1, ssIlg(c - b));
            first = c;
          } else {
            stack[ssize++] = new StackElement(first, a, depth, limit);
            stack[ssize++] = new StackElement(c, last, depth, limit);
            first = b;
            last = c;
            depth += 1;
            limit = ssIlg(c - b);
          }
        }

      } else {
        limit += 1;
        if (T[Td + SA[PA + SA[first]] - 1] < v) {
          first = ssPartition(PA, first, last, depth);
          limit = ssIlg(last - first);
        }
        depth += 1;
      }

    }

  }

  /**
   * Returns the pivot element.
   */
  private int ssPivot(int Td, int PA, int first, int last) {
    int middle;// SA pointer
    int t = last - first;
    middle = first + t / 2;

    if (t <= 512) {
      if (t <= 32) {
        return ssMedian3(Td, PA, first, middle, last - 1);
      } else {
        t >>= 2;
        return ssMedian5(Td, PA, first, first + t, middle, last - 1 - t, last - 1);
      }
    }
    t >>= 3;
    first = ssMedian3(Td, PA, first, first + t, first + (t << 1));
    middle = ssMedian3(Td, PA, middle - t, middle, middle + t);
    last = ssMedian3(Td, PA, last - 1 - (t << 1), last - 1 - t, last - 1);
    return ssMedian3(Td, PA, first, middle, last);
  }

  /**
   * Returns the median of five elements
   */
  private int ssMedian5(int Td, int PA, int v1, int v2, int v3, int v4, int v5) {
    int t;
    if (T[Td + SA[PA + SA[v2]]] > T[Td + SA[PA + SA[v3]]]) {
      t = v2;
      v2 = v3;
      v3 = t;

    }
    if (T[Td + SA[PA + SA[v4]]] > T[Td + SA[PA + SA[v5]]]) {
      t = v4;
      v4 = v5;
      v5 = t;
    }
    if (T[Td + SA[PA + SA[v2]]] > T[Td + SA[PA + SA[v4]]]) {
      t = v2;
      v4 = t;
      t = v3;
      v3 = v5;
      v5 = t;
    }
    if (T[Td + SA[PA + SA[v1]]] > T[Td + SA[PA + SA[v3]]]) {
      t = v1;
      v1 = v3;
      v3 = t;
    }
    if (T[Td + SA[PA + SA[v1]]] > T[Td + SA[PA + SA[v4]]]) {
      t = v1;
      v4 = t;
      v3 = v5;
    }
    if (T[Td + SA[PA + SA[v3]]] > T[Td + SA[PA + SA[v4]]]) {
      return v4;
    }
    return v3;
  }

  /**
   * Returns the median of three elements.
   */
  private int ssMedian3(int Td, int PA, int v1, int v2, int v3) {
    if (T[Td + SA[PA + SA[v1]]] > T[Td + SA[PA + SA[v2]]]) {
      int t = v1;
      v1 = v2;
      v2 = t;
    }
    if (T[Td + SA[PA + SA[v2]]] > T[Td + SA[PA + SA[v3]]]) {
      if (T[Td + SA[PA + SA[v1]]] > T[Td + SA[PA + SA[v3]]]) {
        return v1;
      } else {
        return v3;
      }
    }
    return v2;
  }

  /**
   * Binary partition for substrings.
   */
  private int ssPartition(int PA, int first, int last, int depth) {
    int a, b;// SA pointer
    int t;
    for (a = first - 1, b = last; ; ) {
      for (; (++a < b) && ((SA[PA + SA[a]] + depth) >= (SA[PA + SA[a] + 1] + 1)); ) {
        SA[a] = ~SA[a];
      }
      for (; (a < --b) && ((SA[PA + SA[b]] + depth) < (SA[PA + SA[b] + 1] + 1)); ) {
      }
      if (b <= a) {
        break;
      }
      t = ~SA[b];
      SA[b] = SA[a];
      SA[a] = t;
    }
    if (first < a) {
      SA[first] = ~SA[first];
    }
    return a;
  }

  /**
   * Simple top-down heapsort.
   */
  private void ssHeapSort(int Td, int PA, int sa, int size) {
    int i, m, t;

    m = size;
    if ((size % 2) == 0) {
      m--;
      if (T[Td + SA[PA + SA[sa + (m / 2)]]] < T[Td + SA[PA + SA[sa + m]]]) {
        swapInSA(sa + m, sa + (m / 2));
      }
    }

    for (i = m / 2 - 1; 0 <= i; --i) {
      ssFixDown(Td, PA, sa, i, m);
    }
    if ((size % 2) == 0) {
      swapInSA(sa, sa + m);
      ssFixDown(Td, PA, sa, 0, m);
    }
    for (i = m - 1; 0 < i; --i) {
      t = SA[sa];
      SA[sa] = SA[sa + i];
      ssFixDown(Td, PA, sa, 0, i);
      SA[sa + i] = t;
    }

  }

  /**
   *
   */
  private void ssFixDown(int Td, int PA, int sa, int i, int size) {
    int j, k;
    int v;
    int c, d, e;

    for (
      v = SA[sa + i], c = T[Td + SA[PA + v]];
      (j = 2 * i + 1) < size; SA[sa + i] = SA[sa + k], i = k) {
      d = T[Td + SA[PA + SA[sa + (k = j++)]]];
      if (d < (e = T[Td + SA[PA + SA[sa + j]]])) {
        k = j;
        d = e;
      }
      if (d <= c) {
        break;
      }
    }
    SA[i + sa] = v;

  }

  /**
   *
   */
  private static int ssIlg(int n) {

    return ((n & 0xff00) != 0) ? (8 + lg_table[(n >> 8) & 0xff]) : lg_table[n & 0xff];
  }

  /**
   *
   */
  private void swapInSA(int a, int b) {
    int tmp = SA[a];
    SA[a] = SA[b];
    SA[b] = tmp;
  }

  /**
   * Tandem repeat sort
   */
  private void trSort(int ISA, int n, int depth) {
    TRBudget budget = new TRBudget(trIlg(n) * 2 / 3, n);
    int ISAd;
    int first, last;// SA pointers
    int t, skip, unsorted;
    for (ISAd = ISA + depth; -n < SA[0]; ISAd += ISAd - ISA) {
      first = 0;
      skip = 0;
      unsorted = 0;
      do {
        if ((t = SA[first]) < 0) {
          first -= t;
          skip += t;
        } else {
          if (skip != 0) {
            SA[first + skip] = skip;
            skip = 0;
          }
          last = SA[ISA + t] + 1;
          if (1 < (last - first)) {
            budget.count = 0;
            trIntroSort(ISA, ISAd, first, last, budget);
            if (budget.count != 0) {
              unsorted += budget.count;
            } else {
              skip = first - last;
            }
          } else if ((last - first) == 1) {
            skip = -1;
          }
          first = last;
        }
      } while (first < n);
      if (skip != 0) {
        SA[first + skip] = skip;
      }
      if (unsorted == 0) {
        break;
      }
    }
  }

  /**
   *
   */
  private TRPartitionResult trPartition(int ISAd, int first, int middle, int last, int v) {
    int a, b, c, d, e, f;// ptr
    int t, s, x = 0;

    for (b = middle - 1; (++b < last) && ((x = SA[ISAd + SA[b]]) == v); ) {
    }
    if (((a = b) < last) && (x < v)) {
      for (; (++b < last) && ((x = SA[ISAd + SA[b]]) <= v); ) {
        if (x == v) {
          swapInSA(a, b);
          ++a;
        }
      }
    }
    for (c = last; (b < --c) && ((x = SA[ISAd + SA[c]]) == v); ) {
    }
    if ((b < (d = c)) && (x > v)) {
      for (; (b < --c) && ((x = SA[ISAd + SA[c]]) >= v); ) {
        if (x == v) {
          swapInSA(c, d);
          --d;
        }
      }
    }
    for (; b < c; ) {
      swapInSA(c, b);
      for (; (++b < c) && ((x = SA[ISAd + SA[b]]) <= v); ) {
        if (x == v) {
          swapInSA(a, b);
          ++a;
        }
      }
      for (; (b < --c) && ((x = SA[ISAd + SA[c]]) >= v); ) {
        if (x == v) {
          swapInSA(c, d);
          --d;
        }
      }
    }

    if (a <= d) {
      c = b - 1;
      if ((s = a - first) > (t = b - a)) {
        s = t;
      }
      for (e = first, f = b - s; 0 < s; --s, ++e, ++f) {
        swapInSA(e, f);
      }
      if ((s = d - c) > (t = last - d - 1)) {
        s = t;
      }
      for (e = b, f = last - s; 0 < s; --s, ++e, ++f) {
        swapInSA(e, f);
      }
      first += (b - a);
      last -= (d - c);
    }
    return new TRPartitionResult(first, last);
  }

  private void trIntroSort(int ISA, int ISAd, int first, int last, TRBudget budget) {
    final int STACK_SIZE = TR_STACKSIZE;
    StackElement[] stack = new StackElement[STACK_SIZE];
    int a, b, c;// pointers
    int v, x;
    int incr = ISAd - ISA;
    int limit, next;
    int ssize, trlink = -1;
    for (ssize = 0, limit = trIlg(last - first); ; ) {
      if (limit < 0) {
        if (limit == -1) {
                    /* tandem repeat partition */
          TRPartitionResult res = trPartition(ISAd - incr, first, first, last, last - 1);
          a = res.a;
          b = res.b;
                    /* update ranks */
          if (a < last) {
            for (c = first, v = a - 1; c < a; ++c) {
              SA[ISA + SA[c]] = v;
            }
          }
          if (b < last) {
            for (c = a, v = b - 1; c < b; ++c) {
              SA[ISA + SA[c]] = v;
            }
          }

                    /* push */
          if (1 < (b - a)) {
            stack[ssize++] = new StackElement(0, a, b, 0, 0);
            stack[ssize++] = new StackElement(ISAd - incr, first, last, -2, trlink);
            trlink = ssize - 2;
          }
          if ((a - first) <= (last - b)) {
            if (1 < (a - first)) {
              stack[ssize++] = new StackElement(ISAd, b, last, trIlg(last - b), trlink);
              last = a;
              limit = trIlg(a - first);
            } else if (1 < (last - b)) {
              first = b;
              limit = trIlg(last - b);
            } else {
              if (ssize > 0) {
                StackElement se = stack[--ssize];
                ISAd = se.a;
                first = se.b;
                last = se.c;
                limit = se.d;
                trlink = se.e;
              } else {
                return;
              }

            }
          } else {
            if (1 < (last - b)) {
              stack[ssize++] = new StackElement(ISAd, first, a, trIlg(a - first), trlink);
              first = b;
              limit = trIlg(last - b);
            } else if (1 < (a - first)) {
              last = a;
              limit = trIlg(a - first);
            } else {
              if (ssize > 0) {
                StackElement se = stack[--ssize];
                ISAd = se.a;
                first = se.b;
                last = se.c;
                limit = se.d;
                trlink = se.e;
              } else {
                return;
              }
            }
          }
        } else if (limit == -2) {
                    /* tandem repeat copy */
          StackElement se = stack[--ssize];
          a = se.b;
          b = se.c;
          if (stack[ssize].d == 0) {
            trCopy(ISA, first, a, b, last, ISAd - ISA);
          } else {
            if (0 <= trlink) {
              stack[trlink].d = -1;
            }
            trPartialCopy(ISA, first, a, b, last, ISAd - ISA);
          }
          if (ssize > 0) {
            se = stack[--ssize];
            ISAd = se.a;
            first = se.b;
            last = se.c;
            limit = se.d;
            trlink = se.e;
          } else {
            return;
          }
        } else {
                    /* sorted partition */
          if (0 <= SA[first]) {
            a = first;
            do {
              SA[ISA + SA[a]] = a;
            } while ((++a < last) && (0 <= SA[a]));
            first = a;
          }
          if (first < last) {
            a = first;
            do {
              SA[a] = ~SA[a];
            } while (SA[++a] < 0);
            next = (SA[ISA + SA[a]] != SA[ISAd + SA[a]]) ? trIlg(a - first + 1) : -1;
            if (++a < last) {
              for (b = first, v = a - 1; b < a; ++b) {
                SA[ISA + SA[b]] = v;
              }
            }

                        /* push */
            if (budget.check(a - first) != 0) {
              if ((a - first) <= (last - a)) {
                stack[ssize++] = new StackElement(ISAd, a, last, -3, trlink);
                ISAd += incr;
                last = a;
                limit = next;
              } else {
                if (1 < (last - a)) {
                  stack[ssize++] = new StackElement(ISAd + incr, first, a, next, trlink);
                  first = a;
                  limit = -3;
                } else {
                  ISAd += incr;
                  last = a;
                  limit = next;
                }
              }
            } else {
              if (0 <= trlink) {
                stack[trlink].d = -1;
              }
              if (1 < (last - a)) {
                first = a;
                limit = -3;
              } else {
                if (ssize > 0) {
                  StackElement se = stack[--ssize];
                  ISAd = se.a;
                  first = se.b;
                  last = se.c;
                  limit = se.d;
                  trlink = se.e;
                } else {
                  return;
                }
              }
            }
          } else {
            if (ssize > 0) {
              StackElement se = stack[--ssize];
              ISAd = se.a;
              first = se.b;
              last = se.c;
              limit = se.d;
              trlink = se.e;
            } else {
              return;
            }
          }
        }
        continue;
      }

      if ((last - first) <= TR_INSERTIONSORT_THRESHOLD) {
        trInsertionSort(ISAd, first, last);
        limit = -3;
        continue;
      }

      if (limit-- == 0) {
        trHeapSort(ISAd, first, last - first);
        for (a = last - 1; first < a; a = b) {
          for (x = SA[ISAd + SA[a]], b = a - 1; (first <= b) && (SA[ISAd + SA[b]] == x); --b) {
            SA[b] = ~SA[b];
          }
        }
        limit = -3;
        continue;
      }
      // choose pivot
      a = trPivot(ISAd, first, last);
      swapInSA(first, a);
      v = SA[ISAd + SA[first]];

      // partition
      TRPartitionResult res = trPartition(ISAd, first, first + 1, last, v);
      a = res.a;
      b = res.b;

      if ((last - first) != (b - a)) {
        next = (SA[ISA + SA[a]] != v) ? trIlg(b - a) : -1;

                /* update ranks */
        for (c = first, v = a - 1; c < a; ++c) {
          SA[ISA + SA[c]] = v;
        }
        if (b < last) {
          for (c = a, v = b - 1; c < b; ++c) {
            SA[ISA + SA[c]] = v;
          }
        }

                /* push */
        if ((1 < (b - a)) && ((budget.check(b - a) != 0))) {
          if ((a - first) <= (last - b)) {
            if ((last - b) <= (b - a)) {
              if (1 < (a - first)) {
                stack[ssize++] = new StackElement(ISAd + incr, a, b, next, trlink);
                stack[ssize++] = new StackElement(ISAd, b, last, limit, trlink);
                last = a;
              } else if (1 < (last - b)) {
                stack[ssize++] = new StackElement(ISAd + incr, a, b, next, trlink);
                first = b;
              } else {
                ISAd += incr;
                first = a;
                last = b;
                limit = next;
              }
            } else if ((a - first) <= (b - a)) {
              if (1 < (a - first)) {
                stack[ssize++] = new StackElement(ISAd, b, last, limit, trlink);
                stack[ssize++] = new StackElement(ISAd + incr, a, b, next, trlink);
                last = a;
              } else {
                stack[ssize++] = new StackElement(ISAd, b, last, limit, trlink);
                ISAd += incr;
                first = a;
                last = b;
                limit = next;
              }
            } else {
              stack[ssize++] = new StackElement(ISAd, b, last, limit, trlink);
              stack[ssize++] = new StackElement(ISAd, first, a, limit, trlink);
              ISAd += incr;
              first = a;
              last = b;
              limit = next;
            }
          } else {
            if ((a - first) <= (b - a)) {
              if (1 < (last - b)) {
                stack[ssize++] = new StackElement(ISAd + incr, a, b, next, trlink);
                stack[ssize++] = new StackElement(ISAd, first, a, limit, trlink);
                first = b;
              } else if (1 < (a - first)) {
                stack[ssize++] = new StackElement(ISAd + incr, a, b, next, trlink);
                last = a;
              } else {
                ISAd += incr;
                first = a;
                last = b;
                limit = next;
              }
            } else if ((last - b) <= (b - a)) {
              if (1 < (last - b)) {
                stack[ssize++] = new StackElement(ISAd, first, a, limit, trlink);
                stack[ssize++] = new StackElement(ISAd + incr, a, b, next, trlink);
                first = b;
              } else {
                stack[ssize++] = new StackElement(ISAd, first, a, limit, trlink);
                ISAd += incr;
                first = a;
                last = b;
                limit = next;
              }
            } else {
              stack[ssize++] = new StackElement(ISAd, first, a, limit, trlink);
              stack[ssize++] = new StackElement(ISAd, b, last, limit, trlink);
              ISAd += incr;
              first = a;
              last = b;
              limit = next;
            }
          }
        } else {
          if ((1 < (b - a)) && (0 <= trlink)) {
            stack[trlink].d = -1;
          }
          if ((a - first) <= (last - b)) {
            if (1 < (a - first)) {
              stack[ssize++] = new StackElement(ISAd, b, last, limit, trlink);
              last = a;
            } else if (1 < (last - b)) {
              first = b;
            } else {
              if (ssize > 0) {
                StackElement se = stack[--ssize];
                ISAd = se.a;
                first = se.b;
                last = se.c;
                limit = se.d;
                trlink = se.e;
              } else {
                return;
              }
            }
          } else {
            if (1 < (last - b)) {
              stack[ssize++] = new StackElement(ISAd, first, a, limit, trlink);
              first = b;
            } else if (1 < (a - first)) {
              last = a;
            } else {
              if (ssize > 0) {
                StackElement se = stack[--ssize];
                ISAd = se.a;
                first = se.b;
                last = se.c;
                limit = se.d;
                trlink = se.e;
              } else {
                return;
              }
            }
          }
        }
      } else {
        if (budget.check(last - first) != 0) {
          limit = trIlg(last - first);
          ISAd += incr;
        } else {
          if (0 <= trlink) {
            stack[trlink].d = -1;
          }
          if (ssize > 0) {
            StackElement se = stack[--ssize];
            ISAd = se.a;
            first = se.b;
            last = se.c;
            limit = se.d;
            trlink = se.e;
          } else {
            return;
          }
        }
      }

    }

  }

  /**
   * Returns the pivot element.
   */
  private int trPivot(int ISAd, int first, int last) {
    int middle;
    int t;

    t = last - first;
    middle = first + t / 2;

    if (t <= 512) {
      if (t <= 32) {
        return trMedian3(ISAd, first, middle, last - 1);
      } else {
        t >>= 2;
        return trMedian5(ISAd, first, first + t, middle, last - 1 - t, last - 1);
      }
    }
    t >>= 3;
    first = trMedian3(ISAd, first, first + t, first + (t << 1));
    middle = trMedian3(ISAd, middle - t, middle, middle + t);
    last = trMedian3(ISAd, last - 1 - (t << 1), last - 1 - t, last - 1);
    return trMedian3(ISAd, first, middle, last);
  }

  /**
   * Returns the median of five elements.
   */
  private int trMedian5(int ISAd, int v1, int v2, int v3, int v4, int v5) {
    int t;
    if (SA[ISAd + SA[v2]] > SA[ISAd + SA[v3]]) {
      t = v2;
      v2 = v3;
      v3 = t;
    }
    if (SA[ISAd + SA[v4]] > SA[ISAd + SA[v5]]) {
      t = v4;
      v4 = v5;
      v5 = t;
    }
    if (SA[ISAd + SA[v2]] > SA[ISAd + SA[v4]]) {
      t = v2;
      v4 = t;
      t = v3;
      v3 = v5;
      v5 = t;
    }
    if (SA[ISAd + SA[v1]] > SA[ISAd + SA[v3]]) {
      t = v1;
      v1 = v3;
      v3 = t;
    }
    if (SA[ISAd + SA[v1]] > SA[ISAd + SA[v4]]) {
      t = v1;
      v4 = t;
      v3 = v5;
    }
    if (SA[ISAd + SA[v3]] > SA[ISAd + SA[v4]]) {
      return v4;
    }
    return v3;
  }

  /**
   * Returns the median of three elements.
   */
  private int trMedian3(int ISAd, int v1, int v2, int v3) {
    if (SA[ISAd + SA[v1]] > SA[ISAd + SA[v2]]) {
      int t = v1;
      v1 = v2;
      v2 = t;
    }
    if (SA[ISAd + SA[v2]] > SA[ISAd + SA[v3]]) {
      if (SA[ISAd + SA[v1]] > SA[ISAd + SA[v3]]) {
        return v1;
      } else {
        return v3;
      }
    }
    return v2;
  }

  /**
   *
   */
  private void trHeapSort(int ISAd, int sa, int size) {
    int i, m, t;

    m = size;
    if ((size % 2) == 0) {
      m--;
      if (SA[ISAd + SA[sa + m / 2]] < SA[ISAd + SA[sa + m]]) {
        swapInSA(sa + m, sa + m / 2);
      }
    }

    for (i = m / 2 - 1; 0 <= i; --i) {
      trFixDown(ISAd, sa, i, m);
    }
    if ((size % 2) == 0) {
      swapInSA(sa, sa + m);
      trFixDown(ISAd, sa, 0, m);
    }
    for (i = m - 1; 0 < i; --i) {
      t = SA[sa];
      SA[sa] = SA[sa + i];
      trFixDown(ISAd, sa, 0, i);
      SA[sa + i] = t;
    }

  }

  /**
   *
   */
  private void trFixDown(int ISAd, int sa, int i, int size) {
    int j, k;
    int v;
    int c, d, e;

    for (v = SA[sa + i], c = SA[ISAd + v]; (j = 2 * i + 1) < size; SA[sa + i] = SA[sa + k], i = k) {
      d = SA[ISAd + SA[sa + (k = j++)]];
      if (d < (e = SA[ISAd + SA[sa + j]])) {
        k = j;
        d = e;
      }
      if (d <= c) {
        break;
      }
    }
    SA[sa + i] = v;

  }

  /**
   */
  private void trInsertionSort(int ISAd, int first, int last) {
    int a, b;// SA ptr
    int t, r;

    for (a = first + 1; a < last; ++a) {
      for (t = SA[a], b = a - 1; 0 > (r = SA[ISAd + t] - SA[ISAd + SA[b]]); ) {
        do {
          SA[b + 1] = SA[b];
        } while ((first <= --b) && (SA[b] < 0));
        if (b < first) {
          break;
        }
      }
      if (r == 0) {
        SA[b] = ~SA[b];
      }
      SA[b + 1] = t;
    }

  }

  /**
   */
  private void trPartialCopy(int ISA, int first, int a, int b, int last, int depth) {
    int c, d, e;// ptr
    int s, v;
    int rank, lastrank, newrank = -1;

    v = b - 1;
    lastrank = -1;
    for (c = first, d = a - 1; c <= d; ++c) {
      if ((0 <= (s = SA[c] - depth)) && (SA[ISA + s] == v)) {
        SA[++d] = s;
        rank = SA[ISA + s + depth];
        if (lastrank != rank) {
          lastrank = rank;
          newrank = d;
        }
        SA[ISA + s] = newrank;
      }
    }

    lastrank = -1;
    for (e = d; first <= e; --e) {
      rank = SA[ISA + SA[e]];
      if (lastrank != rank) {
        lastrank = rank;
        newrank = e;
      }
      if (newrank != rank) {
        SA[ISA + SA[e]] = newrank;
      }
    }

    lastrank = -1;
    for (c = last - 1, e = d + 1, d = b; e < d; --c) {
      if ((0 <= (s = SA[c] - depth)) && (SA[ISA + s] == v)) {
        SA[--d] = s;
        rank = SA[ISA + s + depth];
        if (lastrank != rank) {
          lastrank = rank;
          newrank = d;
        }
        SA[ISA + s] = newrank;
      }
    }

  }

  /**
   * sort suffixes of middle partition by using sorted order of suffixes of left and
   * right partition.
   */
  private void trCopy(int ISA, int first, int a, int b, int last, int depth) {
    int c, d, e;// ptr
    int s, v;

    v = b - 1;
    for (c = first, d = a - 1; c <= d; ++c) {
      s = SA[c] - depth;
      if ((0 <= s) && (SA[ISA + s] == v)) {
        SA[++d] = s;
        SA[ISA + s] = d;
      }
    }
    for (c = last - 1, e = d + 1, d = b; e < d; --c) {
      s = SA[c] - depth;
      if ((0 <= s) && (SA[ISA + s] == v)) {
        SA[--d] = s;
        SA[ISA + s] = d;
      }
    }
  }

  /**
   *
   */
  private static int trIlg(int n) {
    return ((n & 0xffff0000) != 0) ?
      (((n & 0xff000000) != 0) ?
        24 + lg_table[(n >> 24) & 0xff] :
        16 + lg_table[(n >> 16) & 0xff]) :
      (n & 0x0000ff00) != 0 ? 8 + lg_table[n >> 8 & 0xff] : lg_table[n & 0xff];
  }

}
