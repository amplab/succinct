package edu.berkeley.cs.succinct.wavelettree;

import edu.berkeley.cs.succinct.bitmap.BitMap;
import edu.berkeley.cs.succinct.dictionary.Dictionary;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class WaveletTree {

    private WaveletNode root;
    private long wavelet_write_offset = 0;

    public WaveletTree(WaveletNode root) {
        this.root = root;
    }

    public WaveletTree(long s, long e, ArrayList<Long> v, ArrayList<Long> v_c) {
        assert (v.size() > 0);
        assert (v_c.size() == v.size());
        if (s == e)
            this.root = null;
        else
            this.root = new WaveletNode(s, e, v, v_c);
    }

    public static class WaveletNode {
        public WaveletNode left, right;
        public char id;
        public Dictionary D;

        public WaveletNode(long s, long e, ArrayList<Long> v,
                ArrayList<Long> v_c) {
            assert (v.size() > 0);
            assert (v.size() == v_c.size());
            char m = (char) v_c.get((v.size() - 1) / 2).intValue();
            m = (char) Math.min(m, e - 1);

            long r;
            ArrayList<Long> v1, v1_c, v0, v0_c;

            v1 = new ArrayList<Long>();
            v1_c = new ArrayList<Long>();
            v0 = new ArrayList<Long>();
            v0_c = new ArrayList<Long>();
            BitMap B = new BitMap(v.size());

            this.id = m;

            for (int i = 0; i < v.size(); i++) {
                if (v_c.get(i) > m && v_c.get(i) <= e) {
                    B.setBit(v.get(i).intValue());
                }
            }

            this.D = new Dictionary(B);

            for (long i = 0; i < v.size(); i++) {
                if (v_c.get((int) i) > m && v_c.get((int) i) <= e) {
                    r = D.getRank1(v.get((int) i).intValue()) - 1;
                    assert (r >= 0);
                    v1.add(r);
                    v1_c.add(v_c.get((int) i));
                } else {
                    r = D.getRank0(v.get((int) i).intValue()) - 1;
                    assert (r >= 0);
                    v0.add(r);
                    v0_c.add(v_c.get((int) i));
                }
            }

            if (s == m)
                this.left = null;
            else
                this.left = new WaveletNode(s, m, v0, v0_c);
            v0.clear();
            v0_c.clear();

            if (m + 1 == e)
                this.right = null;
            else
                this.right = new WaveletNode(m + 1, e, v1, v1_c);
            v1.clear();
            v1_c.clear();
        }
    }

    private int findSizeOfSerializedNode(WaveletNode root) {
        int size = 0;

        if (root == null) {
            return size;
        }

        // Adding up bytes for current node
        size += 1; // ID size
        size += 2 * 8; // Offsets for left and right
        size += 8; // Size of Dictionary
        size += 8 * (root.D.rankL3.length + root.D.posL3.length);
        // RANKL3, POSL3
        size += 8 * (root.D.rankL12.length + root.D.posL12.length);
        // RANKL12, POSL12
        size += 8; // Size of bitmap
        size += 8 * root.D.bitMap.data.length; // Bitmap

        return size;
    }

    private int findSizeOfSerializedTree(WaveletNode root) {

        if (root == null) {
            return 0;
        }
        int size = findSizeOfSerializedNode(root);
        size += findSizeOfSerializedTree(root.left);
        size += findSizeOfSerializedTree(root.right);

        return size;
    }

    private long findOffsetLeft(WaveletNode root) {
        return findSizeOfSerializedNode(root);
    }

    private long findOffsetRight(WaveletNode root) {
        return findSizeOfSerializedNode(root)
                + findSizeOfSerializedTree(root.left);
    }

    private ByteBuffer getWaveletNodeByteBuffer(WaveletNode root, ByteBuffer buf) {

        buf.put((byte) root.id);

        // Write byte offset to left child, right child
        long offset_left = 0, offset_right = 0;
        if (root.left != null) {
            offset_left = wavelet_write_offset + findOffsetLeft(root);
        }
        if (root.right != null) {
            offset_right = wavelet_write_offset + findOffsetRight(root);
        }

        buf.putLong(offset_left);
        buf.putLong(offset_right);

        buf.putLong(root.D.size);
        for (int i = 0; i < root.D.rankL3.length; i++) {
            buf.putLong(root.D.rankL3[i]);
        }
        for (int i = 0; i < root.D.posL3.length; i++) {
            buf.putLong(root.D.posL3[i]);
        }
        for (int i = 0; i < root.D.rankL12.length; i++) {
            buf.putLong(root.D.rankL12[i]);
        }
        for (int i = 0; i < root.D.posL12.length; i++) {
            buf.putLong(root.D.posL12[i]);
        }
        buf.putLong(root.D.bitMap.size);
        for (int i = 0; i < root.D.bitMap.data.length; i++) {
            buf.putLong(root.D.bitMap.data[i]);
        }

        wavelet_write_offset += findSizeOfSerializedNode(root);

        return buf;
    }

    private ByteBuffer getWaveletTreeByteBuffer(WaveletNode root, ByteBuffer buf) {
        if (root == null) {
            return buf;
        }

        buf = getWaveletNodeByteBuffer(root, buf);
        buf = getWaveletTreeByteBuffer(root.left, buf);
        buf = getWaveletTreeByteBuffer(root.right, buf);
        return buf;
    }

    public ByteBuffer getByteBuffer() {
        int size = findSizeOfSerializedTree(root);
        if (size == 0)
            return null;
        ByteBuffer waveletTreeBuf = (ByteBuffer) getWaveletTreeByteBuffer(root,
                ByteBuffer.allocate(size)).flip();
        return waveletTreeBuf;
    }
}
