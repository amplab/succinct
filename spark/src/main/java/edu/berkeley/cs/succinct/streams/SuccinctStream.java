package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctCore;
import edu.berkeley.cs.succinct.util.BitUtils;
import edu.berkeley.cs.succinct.util.CommonUtils;
import edu.berkeley.cs.succinct.util.SuccinctConstants;
import edu.berkeley.cs.succinct.util.container.Pair;
import edu.berkeley.cs.succinct.util.stream.DeltaEncodedIntStream;
import edu.berkeley.cs.succinct.util.stream.LongArrayStream;
import edu.berkeley.cs.succinct.util.stream.serops.ArrayOps;
import edu.berkeley.cs.succinct.util.stream.serops.IntVectorOps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;

/**
 * Stream based implementation for Succinct algorithms
 */
public class SuccinctStream extends SuccinctCore {

  protected transient LongArrayStream sa;
  protected transient LongArrayStream isa;
  protected transient LongArrayStream columnoffsets;
  protected transient DeltaEncodedIntStream[] columns;

  protected transient FSDataInputStream originalStream;
  protected transient long endOfCoreStream;

  private transient Configuration conf;

  /**
   * Constructor to map a file containing Succinct data structures via stream.
   *
   * @param filePath Path of the file.
   * @param conf     Configuration for the filesystem.
   * @throws IOException
   */
  public SuccinctStream(Path filePath, Configuration conf) throws IOException {
    this.conf = conf;

    FSDataInputStream is = getStream(filePath);

    setOriginalSize(is.readInt());
    setSamplingRateSA(is.readInt());
    setSamplingRateISA(is.readInt());
    setSamplingRateNPA(is.readInt());
    setSampleBitWidth(is.readInt());
    setAlphabetSize(is.readInt());

    // Deserialize alphabetmap
    alphabetMap = new HashMap<>();
    for (int i = 0; i < getAlphabetSize() + 1; i++) {
      byte c = is.readByte();
      int v1 = is.readInt();
      int v2 = is.readInt();
      alphabetMap.put(c, new Pair<>(v1, v2));
    }

    // Read alphabet
    alphabet = new byte[getAlphabetSize()];
    int read = is.read(alphabet);
    assert read == getAlphabetSize();

    // Compute number of sampled elements
    int totalSampledBitsSA =
      CommonUtils.numBlocks(getOriginalSize(), getSamplingRateSA()) * getSampleBitWidth();
    int saSize = BitUtils.bitsToBlocks64(totalSampledBitsSA) * SuccinctConstants.LONG_SIZE_BYTES;

    // Map SA
    sa = new LongArrayStream(is, is.getPos(), saSize);
    is.seek(is.getPos() + saSize);

    // Compute number of sampled elements
    int totalSampledBitsISA =
      CommonUtils.numBlocks(getOriginalSize(), getSamplingRateISA()) * getSampleBitWidth();
    int isaSize = BitUtils.bitsToBlocks64(totalSampledBitsISA) * SuccinctConstants.LONG_SIZE_BYTES;

    // Map ISA
    isa = new LongArrayStream(is, is.getPos(), isaSize);
    is.seek(is.getPos() + isaSize);

    // Map columnoffsets
    int columnoffsetsSize = getAlphabetSize() * SuccinctConstants.LONG_SIZE_BYTES;
    columnoffsets = new LongArrayStream(is, is.getPos(), columnoffsetsSize);
    is.seek(is.getPos() + columnoffsetsSize);

    columns = new DeltaEncodedIntStream[getAlphabetSize()];
    for (int i = 0; i < getAlphabetSize(); i++) {
      int columnSize = is.readInt();
      assert columnSize != 0;
      columns[i] = new DeltaEncodedIntStream(is, is.getPos());
      is.seek(is.getPos() + columnSize);
    }

    endOfCoreStream = is.getPos();

    is.seek(0);
    this.originalStream = is;
  }

  /**
   * Constructor to map a file containing Succinct data structures via stream
   *
   * @param filePath Path of the file.
   * @throws IOException
   */
  public SuccinctStream(Path filePath) throws IOException {
    this(filePath, new Configuration());
  }

  /**
   * Opens a new FSDataInputStream on the provided file.
   *
   * @param path Path of the file.
   * @return A FSDataInputStream.
   * @throws IOException
   */
  protected FSDataInputStream getStream(Path path) throws IOException {
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    return fs.open(path);
  }

  @Override public int getSuccinctSize() {
    return baseSize()
      + (12 + 12 + SuccinctConstants.REF_SIZE_BYTES) * 3
      + (12 + columns.length * (2 * SuccinctConstants.REF_SIZE_BYTES + 8));
  }

  /**
   * Lookup NPA at specified index.
   *
   * @param i Index into NPA.
   * @return Value of NPA at specified index.
   */
  @Override public long lookupNPA(long i) {

    if (i > getOriginalSize() - 1 || i < 0) {
      throw new ArrayIndexOutOfBoundsException(
        "NPA index out of bounds: i = " + i + " originalSize = " + getOriginalSize());
    }

    try {
      int colId = ArrayOps.getRank1(columnoffsets, 0, getAlphabetSize(), (int) i) - 1;

      assert colId < getAlphabetSize();
      assert columnoffsets.get(colId) <= i;

      return (long) columns[colId].get((int) (i - columnoffsets.get(colId)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Lookup SA at specified index.
   *
   * @param i Index into SA.
   * @return Value of SA at specified index.
   */
  @Override public long lookupSA(long i) {

    if (i > getOriginalSize() - 1 || i < 0) {
      throw new ArrayIndexOutOfBoundsException(
        "SA index out of bounds: i = " + i + " originalSize = " + getOriginalSize());
    }

    try {
      int j = 0;
      while (i % getSamplingRateSA() != 0) {
        i = lookupNPA(i);
        j++;
      }
      long saVal = IntVectorOps.get(sa, (int) (i / getSamplingRateSA()), getSampleBitWidth());

      if (saVal < j)
        return getOriginalSize() - (j - saVal);
      return saVal - j;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Lookup ISA at specified index.
   *
   * @param i Index into ISA.
   * @return Value of ISA at specified index.
   */
  @Override public long lookupISA(long i) {

    if (i > getOriginalSize() - 1 || i < 0) {
      throw new ArrayIndexOutOfBoundsException(
        "ISA index out of bounds: i = " + i + " originalSize = " + getOriginalSize());
    }

    try {

      int sampleIdx = (int) (i / getSamplingRateISA());
      int pos = IntVectorOps.get(isa, sampleIdx, getSampleBitWidth());
      i -= (sampleIdx * getSamplingRateISA());
      while (i-- != 0) {
        pos = (int) lookupNPA(pos);
      }
      return pos;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Lookup up the inverted alphabet map at specified index.
   *
   * @param i Index into inverted alphabet map
   * @return Value of inverted alphabet map at specified index.
   */
  @Override public byte lookupC(long i) {

    if (i > getOriginalSize() - 1 || i < 0) {
      throw new ArrayIndexOutOfBoundsException(
        "C index out of bounds: i = " + i + " originalSize = " + getOriginalSize());
    }

    try {
      int idx = ArrayOps.getRank1(columnoffsets, 0, getAlphabetSize(), (int) i) - 1;
      return alphabet[idx];
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Binary Search for a value withing NPA.
   *
   * @param val      Value to be searched.
   * @param startIdx Starting index into NPA.
   * @param endIdx   Ending index into NPA.
   * @param flag     Whether to search for left or the right boundary.
   * @return Search result as an index into the NPA.
   */
  @Override public long binSearchNPA(long val, long startIdx, long endIdx, boolean flag) {

    long sp = startIdx;
    long ep = endIdx;
    long m;

    while (sp <= ep) {
      m = (sp + ep) / 2;

      long npaVal;
      npaVal = lookupNPA(m);

      if (npaVal == val) {
        return m;
      } else if (val < npaVal) {
        ep = m - 1;
      } else {
        sp = m + 1;
      }
    }

    return flag ? ep : sp;
  }

  /**
   * Close all underlying stream.
   */
  void close() throws IOException {
    originalStream.close();
  }
}
