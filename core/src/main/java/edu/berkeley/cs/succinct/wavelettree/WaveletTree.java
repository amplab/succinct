package edu.berkeley.cs.succinct.wavelettree;

import edu.berkeley.cs.succinct.bitmap.BitMap;
import edu.berkeley.cs.succinct.dictionary.Dictionary;
import gnu.trove.list.array.TLongArrayList;

import java.nio.ByteBuffer;

public class WaveletTree {

  private WaveletNode root;
  private long waveletWriteOffset = 0;

  /**
   * Constructor to initialize WaveletTree from an input WaveletNode
   *
   * @param root Input root node.
   */
  public WaveletTree(WaveletNode root) {
    this.root = root;
  }

  /**
   * Constructor to initialize WaveletTree from values and their corresponding columnIds
   *
   * @param startIdx  Starting context index.
   * @param endIdx    Ending context index.
   * @param values    Context values.
   * @param columnIds Column IDs.
   */
  public WaveletTree(long startIdx, long endIdx, TLongArrayList values, TLongArrayList columnIds) {
    assert (values.size() > 0);
    assert (columnIds.size() == values.size());
    if (startIdx == endIdx)
      this.root = null;
    else
      this.root = new WaveletNode(startIdx, endIdx, values, columnIds);
  }

  /**
   * Get size of serialized WaveletNode.
   *
   * @param node Input node.
   * @return Size of the serialized node.
   */
  private int findSizeOfSerializedNode(WaveletNode node) {
    int size = 0;

    if (node == null) {
      return size;
    }

    // Adding up bytes for current node
    size += 1;                                                  // ID size
    size += 2 * 8;                                              // Offsets for left and right
    size += 8;                                                  // Size of Dictionary
    size += 8 * (node.D.rankL3.length + node.D.posL3.length);   // RANKL3, POSL3
    size += 8 * (node.D.rankL12.length + node.D.posL12.length); // RANKL12, POSL12
    size += 8;                                                  // Size of bitmap
    size += 8 * node.D.bitMap.data.length;                      // Bitmap

    return size;
  }

  /**
   * Get size of serialized WaveletTree.
   *
   * @param root Input root node.
   * @return Size of the serialized tree.
   */
  private int findSizeOfSerializedTree(WaveletNode root) {

    if (root == null) {
      return 0;
    }
    int size = findSizeOfSerializedNode(root);
    size += findSizeOfSerializedTree(root.left);
    size += findSizeOfSerializedTree(root.right);

    return size;
  }

  /**
   * Get left offset in serialized output for input node.
   *
   * @param node Input node.
   * @return Left offset in serialized output.
   */
  private long findOffsetLeft(WaveletNode node) {
    return findSizeOfSerializedNode(node);
  }

  /**
   * Get left offset in serialized output for input node.
   *
   * @param node Input node.
   * @return Right offset in serialized output.
   */
  private long findOffsetRight(WaveletNode node) {
    return findSizeOfSerializedNode(node) + findSizeOfSerializedTree(node.left);
  }

  /**
   * Serialize WaveletNode into input ByteBuffer.
   *
   * @param node Input node.
   * @param buf  Input ByteBuffer.
   * @return Updated ByteBuffer with the serialized WaveletNode.
   */
  private ByteBuffer getWaveletNodeByteBuffer(WaveletNode node, ByteBuffer buf) {

    buf.put((byte) node.id);

    // Write byte offset to left child, right child
    long offsetLeft = 0, offsetRight = 0;
    if (node.left != null) {
      offsetLeft = waveletWriteOffset + findOffsetLeft(node);
    }
    if (node.right != null) {
      offsetRight = waveletWriteOffset + findOffsetRight(node);
    }

    buf.putLong(offsetLeft);
    buf.putLong(offsetRight);

    buf.putLong(node.D.size);
    for (int i = 0; i < node.D.rankL3.length; i++) {
      buf.putLong(node.D.rankL3[i]);
    }
    for (int i = 0; i < node.D.posL3.length; i++) {
      buf.putLong(node.D.posL3[i]);
    }
    for (int i = 0; i < node.D.rankL12.length; i++) {
      buf.putLong(node.D.rankL12[i]);
    }
    for (int i = 0; i < node.D.posL12.length; i++) {
      buf.putLong(node.D.posL12[i]);
    }
    buf.putLong(node.D.bitMap.size);
    for (int i = 0; i < node.D.bitMap.data.length; i++) {
      buf.putLong(node.D.bitMap.data[i]);
    }

    waveletWriteOffset += findSizeOfSerializedNode(node);

    return buf;
  }

  /**
   * Serialize WaveletTree into input ByteBuffer.
   *
   * @param root Input root node.
   * @param buf  Input ByteBuffer.
   * @return Updated ByteBuffer with the serialized WaveletTree.
   */
  private ByteBuffer getWaveletTreeByteBuffer(WaveletNode root, ByteBuffer buf) {
    if (root == null) {
      return buf;
    }

    buf = getWaveletNodeByteBuffer(root, buf);
    buf = getWaveletTreeByteBuffer(root.left, buf);
    buf = getWaveletTreeByteBuffer(root.right, buf);
    return buf;
  }

  /**
   * Serialize WaveletTree as a ByteBuffer.
   *
   * @return Serialized WaveletTree as a ByteBuffer.
   */
  public ByteBuffer getByteBuffer() {
    int size = findSizeOfSerializedTree(root);
    if (size == 0)
      return null;
    ByteBuffer waveletTreeBuf =
      (ByteBuffer) getWaveletTreeByteBuffer(root, ByteBuffer.allocate(size)).flip();
    return waveletTreeBuf;
  }

  public static class WaveletNode {
    public WaveletNode left, right;
    public char id;
    public Dictionary D;

    /**
     * Constructor to initialize WaveletNode from values and their corresponding columnIds
     *
     * @param startIdx  Starting context index.
     * @param endIdx    Ending context index.
     * @param values    Context Values.
     * @param columnIds Column IDs.
     */
    public WaveletNode(long startIdx, long endIdx, TLongArrayList values,
      TLongArrayList columnIds) {
      assert (values.size() > 0);
      assert (values.size() == columnIds.size());
      char m = (char) columnIds.get((values.size() - 1) / 2);
      m = (char) Math.min(m, endIdx - 1);

      long r;
      TLongArrayList valuesRight, columnIdsRight, valuesLeft, columnIdsLeft;

      valuesRight = new TLongArrayList();
      columnIdsRight = new TLongArrayList();
      valuesLeft = new TLongArrayList();
      columnIdsLeft = new TLongArrayList();
      BitMap B = new BitMap(values.size());

      this.id = m;

      for (int i = 0; i < values.size(); i++) {
        if (columnIds.get(i) > m && columnIds.get(i) <= endIdx) {
          B.setBit((int) values.get(i));
        }
      }

      this.D = new Dictionary(B);

      for (long i = 0; i < values.size(); i++) {
        if (columnIds.get((int) i) > m && columnIds.get((int) i) <= endIdx) {
          r = D.getRank1((int) values.get((int) i)) - 1;
          assert (r >= 0);
          valuesRight.add(r);
          columnIdsRight.add(columnIds.get((int) i));
        } else {
          r = D.getRank0((int) values.get((int) i)) - 1;
          assert (r >= 0);
          valuesLeft.add(r);
          columnIdsLeft.add(columnIds.get((int) i));
        }
      }

      if (startIdx == m)
        this.left = null;
      else
        this.left = new WaveletNode(startIdx, m, valuesLeft, columnIdsLeft);
      valuesLeft.clear();
      columnIdsLeft.clear();

      if (m + 1 == endIdx)
        this.right = null;
      else
        this.right = new WaveletNode(m + 1, endIdx, valuesRight, columnIdsRight);
      valuesRight.clear();
      columnIdsRight.clear();
    }
  }
}
