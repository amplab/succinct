package succinct;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import succinct.util.SerializedOperations;

public class SuccinctFile extends SuccinctCore {

    /**
	 * 
	 */
    private static final long serialVersionUID = 5879363803993345049L;

    public SuccinctFile(byte[] input, int contextLen) {
        super(input, contextLen);
    }

    public SuccinctFile(String filePath, int contextLen) throws IOException {
        this((new String(Files.readAllBytes(FileSystems.getDefault().getPath(
                "", filePath))) + (char) (1)).getBytes(), contextLen);
    }

    public SuccinctFile(String filePath) throws IOException {
        this(filePath, 3);
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

        char[] buf = new char[len + 1];
        long s;

        s = lookupISA(i);
        int k;
        for (k = 0; k < len; k++) {
            buf[k] = (char) alphabet.get(SerializedOperations.getRank1(
                    coloffsets, 0, sigmaSize, s) - 1);
            s = lookupNPA(s);
        }

        buf[k] = '\0';

        return new String(buf);
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
        Pair<Long, Long> range = new Pair<>(0L, -1L);
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
    private Pair<Long, Long> getRange(char[] p) {

        int m = p.length;
        if (m <= contextLen) {
            return getRangeSlow(p);
        }
        Pair<Long, Long> range = new Pair<>(0L, -1L);
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
            return new ArrayList<>();
        }

        List<Long> positions = new ArrayList<>();
        for (long i = 0; i < ep - sp + 1; i++) {
            positions.add(lookupSA(sp + i));
        }

        return positions;
    }

}
