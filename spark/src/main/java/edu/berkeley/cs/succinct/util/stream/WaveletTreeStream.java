package edu.berkeley.cs.succinct.util.stream;

import edu.berkeley.cs.succinct.util.dictionary.Tables;
import edu.berkeley.cs.succinct.util.CommonUtils;
import edu.berkeley.cs.succinct.util.stream.serops.BitMapOps;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static edu.berkeley.cs.succinct.util.DictionaryUtils.*;

public class WaveletTreeStream {

  private FSDataInputStream stream;
  private long startPos;

  public WaveletTreeStream(FSDataInputStream stream, long startPos) throws IOException {
    this.stream = stream;
    this.startPos = startPos;
  }


  public long lookup(int contextPos, int cellPos, int startIdx, int endIdx) throws IOException {
    stream.seek(startPos);
    return waveletTreeLookup(contextPos, cellPos, startIdx, endIdx);
  }

  private long waveletTreeLookup(int contextPos, int cellPos, int startIdx, int endIdx)
    throws IOException {
    byte m = stream.readByte();
    int left = (int) stream.readLong();
    int right = (int) stream.readLong();
    int dictPos = (int) stream.getPos();
    long p, v;

    if (contextPos > m && contextPos <= endIdx) {
      if (right == 0) {
        return select1(dictPos, cellPos);
      }
      stream.seek(startPos + right);
      p = waveletTreeLookup(contextPos, cellPos, m + 1, endIdx);

      v = select1(dictPos, (int) p);
    } else {
      if (left == 0) {
        return select0(dictPos, cellPos);
      }
      stream.seek(startPos + left);
      p = waveletTreeLookup(contextPos, cellPos, startIdx, m);

      v = select0(dictPos, (int) p);
    }

    return v;
  }

  private long select0(long dictPos, int i) throws IOException {
    assert (i >= 0);

    RandomAccessLongStream dictBuf = new RandomAccessLongStream(stream, dictPos, Integer.MAX_VALUE);

    long size = dictBuf.get();

    long val = i + 1;
    int sp = 0;
    int ep = (int) (size / CommonUtils.two32);
    int m;
    long r;
    int pos = 0;
    int blockClass, blockOffset;
    long sel;
    int lastBlock;
    long rankL12, posL12;

    int l3Size = (int) ((size / CommonUtils.two32) + 1);
    int l12Size = (int) ((size / 2048) + 1);

    int basePos = (int) dictBuf.position();

    while (sp <= ep) {
      m = (sp + ep) / 2;
      r = (m * CommonUtils.two32 - dictBuf.get(basePos + m));
      if (val > r) {
        sp = m + 1;
      } else {
        ep = m - 1;
      }
    }

    ep = Math.max(ep, 0);
    val -= (ep * CommonUtils.two32 - dictBuf.get(basePos + ep));
    pos += dictBuf.get(basePos + l3Size + ep);
    sp = (int) (ep * CommonUtils.two32 / 2048);
    ep = (int) (Math.min(((ep + 1) * CommonUtils.two32 / 2048), Math.ceil((double) size / 2048.0))
      - 1);

    assert (val <= CommonUtils.two32);
    assert (pos >= 0);

    dictBuf.position(basePos + 2 * l3Size);
    basePos = (int) dictBuf.position();

    while (sp <= ep) {
      m = (sp + ep) / 2;
      r = m * 2048 - GETRANKL2(dictBuf.get(basePos + m));
      if (val > r) {
        sp = m + 1;
      } else {
        ep = m - 1;
      }
    }

    ep = Math.max(ep, 0);
    sel = (long) (ep) * 2048L;
    rankL12 = dictBuf.get(basePos + ep);
    posL12 = dictBuf.get(basePos + l12Size + ep);
    val -= (ep * 2048 - GETRANKL2(rankL12));
    pos += GETPOSL2(posL12);

    assert (val <= 2048);
    assert (pos >= 0);

    r = (512 - GETRANKL1(rankL12, 1));
    if (sel + 512 < size && val > r) {
      pos += GETPOSL1(posL12, 1);
      val -= r;
      sel += 512;
      r = (512 - GETRANKL1(rankL12, 2));
      if (sel + 512 < size && val > r) {
        pos += GETPOSL1(posL12, 2);
        val -= r;
        sel += 512;
        r = (512 - GETRANKL1(rankL12, 3));
        if (sel + 512 < size && val > r) {
          pos += GETPOSL1(posL12, 3);
          val -= r;
          sel += 512;
        }
      }
    }

    dictBuf.position(basePos + 2 * l12Size);

    assert (val <= 512);
    assert (pos >= 0);

    dictBuf.get(); // TODO: Could remove this field altogether

    while (true) {
      blockClass = (int) BitMapOps.getValPos(dictBuf, pos, 4);
      short offsetSize = (short) Tables.offsetBits[blockClass];
      pos += 4;
      blockOffset =
        (int) ((blockClass == 0) ? BitMapOps.getBit(dictBuf, pos) * 16 : 0);
      pos += offsetSize;

      if (val <= (16 - (blockClass + blockOffset))) {
        pos -= (4 + offsetSize);
        break;
      }

      val -= (16 - (blockClass + blockOffset));
      sel += 16;
    }

    blockClass = (int) BitMapOps.getValPos(dictBuf, pos, 4);
    pos += 4;
    blockOffset =
      (int) BitMapOps.getValPos(dictBuf, pos, Tables.offsetBits[blockClass]);
    lastBlock = Tables.decodeTable[blockClass][blockOffset];

    long count = 0;
    for (i = 0; i < 16; i++) {
      if (((lastBlock >> (15 - i)) & 1) == 0) {
        count++;
      }
      if (count == val) {
        return sel + i;
      }
    }

    return sel;
  }

  private long select1(long dictPos, int i) throws IOException {
    assert (i >= 0);

    RandomAccessLongStream dictBuf = new RandomAccessLongStream(stream, dictPos, Integer.MAX_VALUE);

    long size = dictBuf.get();

    long val = i + 1;
    int sp = 0;
    int ep = (int) (size / CommonUtils.two32);
    int m;
    long r;
    int pos = 0;
    int blockClass, blockOffset;
    long sel;
    int lastBlock;
    long rankL12, posL12;

    int l3Size = (int) ((size / CommonUtils.two32) + 1);
    int l12Size = (int) ((size / 2048) + 1);
    int basePos = (int) dictBuf.position();

    while (sp <= ep) {
      m = (sp + ep) / 2;
      r = dictBuf.get(basePos + m);
      if (val > r) {
        sp = m + 1;
      } else {
        ep = m - 1;
      }
    }

    ep = Math.max(ep, 0);
    val -= dictBuf.get(basePos + ep);
    pos += dictBuf.get(basePos + l3Size + ep);
    sp = (int) (ep * CommonUtils.two32 / 2048);
    ep = (int) (Math.min(((ep + 1) * CommonUtils.two32 / 2048), Math.ceil((double) size / 2048.0))
      - 1);

    assert (val <= CommonUtils.two32);
    assert (pos >= 0);

    dictBuf.position(basePos + 2 * l3Size);
    basePos = (int) dictBuf.position();

    while (sp <= ep) {
      m = (sp + ep) / 2;
      r = GETRANKL2(dictBuf.get(basePos + m));
      if (val > r) {
        sp = m + 1;
      } else {
        ep = m - 1;
      }
    }

    ep = Math.max(ep, 0);
    sel = (long) (ep) * 2048L;
    rankL12 = dictBuf.get(basePos + ep);
    posL12 = dictBuf.get(basePos + l12Size + ep);
    val -= GETRANKL2(rankL12);
    pos += GETPOSL2(posL12);

    assert (val <= 2048);
    assert (pos >= 0);

    r = GETRANKL1(rankL12, 1);
    if (sel + 512 < size && val > r) {
      pos += GETPOSL1(posL12, 1);
      val -= r;
      sel += 512;
      r = GETRANKL1(rankL12, 2);
      if (sel + 512 < size && val > r) {
        pos += GETPOSL1(posL12, 2);
        val -= r;
        sel += 512;
        r = GETRANKL1(rankL12, 3);
        if (sel + 512 < size && val > r) {
          pos += GETPOSL1(posL12, 3);
          val -= r;
          sel += 512;
        }
      }
    }

    dictBuf.position(basePos + 2 * l12Size);

    assert (val <= 512);
    assert (pos >= 0);

    dictBuf.get(); // TODO: Could remove this field altogether

    while (true) {
      blockClass = (int) BitMapOps.getValPos(dictBuf, pos, 4);
      short offsetSize = (short) Tables.offsetBits[blockClass];
      pos += 4;
      blockOffset =
        (int) ((blockClass == 0) ? BitMapOps.getBit(dictBuf, pos) * 16 : 0);
      pos += offsetSize;

      if (val <= (blockClass + blockOffset)) {
        pos -= (4 + offsetSize);
        break;
      }

      val -= (blockClass + blockOffset);
      sel += 16;
    }

    blockClass = (int) BitMapOps.getValPos(dictBuf, pos, 4);
    pos += 4;
    blockOffset =
      (int) BitMapOps.getValPos(dictBuf, pos, Tables.offsetBits[blockClass]);
    lastBlock = Tables.decodeTable[blockClass][blockOffset];

    long count = 0;
    for (i = 0; i < 16; i++) {
      if (((lastBlock >>> (15 - i)) & 1) == 1) {
        count++;
      }
      if (count == val) {
        return sel + i;
      }
    }

    return sel;
  }

  public void close() throws IOException {
    stream.close();
  }
}
