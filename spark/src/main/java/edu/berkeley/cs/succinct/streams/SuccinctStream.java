package edu.berkeley.cs.succinct.streams;

import edu.berkeley.cs.succinct.SuccinctCore;
import edu.berkeley.cs.succinct.util.container.Pair;
import edu.berkeley.cs.succinct.util.stream.*;
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

  protected transient ByteArrayStream alphabet;
  protected transient LongArrayStream sa;
  protected transient LongArrayStream isa;
  protected transient LongArrayStream neccol;
  protected transient LongArrayStream necrow;
  protected transient LongArrayStream rowoffsets;
  protected transient LongArrayStream coloffsets;
  protected transient LongArrayStream celloffsets;
  protected transient IntArrayStream rowsizes;
  protected transient IntArrayStream colsizes;
  protected transient IntArrayStream roff;
  protected transient IntArrayStream coff;
  protected transient WaveletTreeStream[] wavelettree;

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
    setSampledSASize(is.readInt());
    setAlphaSize(is.readInt());
    setSigmaSize(is.readInt());
    setBits(is.readInt());
    setSampledSABits(is.readInt());
    setSamplingBase(is.readInt());
    setSamplingRate(is.readInt());
    setNumContexts(is.readInt());

    // Read alphabetMap
    alphabetMap = new HashMap<Byte, Pair<Long, Integer>>();
    for (int i = 0; i < this.getAlphaSize(); i++) {
      byte c = is.readByte();
      long v1 = is.readLong();
      int v2 = is.readInt();
      alphabetMap.put(c, new Pair<Long, Integer>(v1, v2));
    }

    // Deserialize contexts
    contextMap = new HashMap<Long, Long>();
    for (int i = 0; i < this.getNumContexts(); i++) {
      long v1 = is.readLong();
      long v2 = is.readLong();
      contextMap.put(v1, v2);
    }

    // Map alphabet
    alphabet = new ByteArrayStream(is, is.getPos(), getAlphaSize());
    is.seek(is.getPos() + getAlphaSize());

    // Map sa
    int saSize = ((getSampledSASize() * getSampledSABits()) / 64 + 1) * 8;
    sa = new LongArrayStream(is, is.getPos(), saSize);
    is.seek(is.getPos() + saSize);

    // Map isa
    int isaSize = ((getSampledSASize() * getSampledSABits()) / 64 + 1) * 8;
    isa = new LongArrayStream(is, is.getPos(), isaSize);
    is.seek(is.getPos() + isaSize);

    // Map neccol
    int neccolSize = is.readInt() * 8;
    neccol = new LongArrayStream(is, is.getPos(), neccolSize);
    is.seek(is.getPos() + neccolSize);

    // Map necrow
    int necrowSize = is.readInt() * 8;
    necrow = new LongArrayStream(is, is.getPos(), necrowSize);
    is.seek(is.getPos() + necrowSize);

    // Map rowoffsets
    int rowoffsetsSize = is.readInt() * 8;
    rowoffsets = new LongArrayStream(is, is.getPos(), rowoffsetsSize);
    is.seek(is.getPos() + rowoffsetsSize);

    // Map coloffsets
    int coloffsetsSize = is.readInt() * 8;
    coloffsets = new LongArrayStream(is, is.getPos(), coloffsetsSize);
    is.seek(is.getPos() + coloffsetsSize);

    // Map celloffsets
    int celloffsetsSize = is.readInt() * 8;
    celloffsets = new LongArrayStream(is, is.getPos(), celloffsetsSize);
    is.seek(is.getPos() + celloffsetsSize);

    // Map rowsizes
    int rowsizesSize = is.readInt() * 4;
    rowsizes = new IntArrayStream(is, is.getPos(), rowsizesSize);
    is.seek(is.getPos() + rowsizesSize);

    // Map colsizes
    int colsizesSize = is.readInt() * 4;
    colsizes = new IntArrayStream(is, is.getPos(), colsizesSize);
    is.seek(is.getPos() + colsizesSize);

    // Map roff
    int roffSize = is.readInt() * 4;
    roff = new IntArrayStream(is, is.getPos(), roffSize);
    is.seek(is.getPos() + roffSize);

    // Map coff
    int coffSize = is.readInt() * 4;
    coff = new IntArrayStream(is, is.getPos(), coffSize);
    is.seek(is.getPos() + coffSize);

    wavelettree = new WaveletTreeStream[getNumContexts()];
    for (int i = 0; i < getNumContexts(); i++) {
      int wavelettreeSize = is.readInt();
      wavelettree[i] = null;
      if (wavelettreeSize != 0) {
        // Map wavelettree
        wavelettree[i] = new WaveletTreeStream(is, is.getPos());
        is.seek(is.getPos() + wavelettreeSize);
      }
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

  /**
   * Lookup NPA at specified index.
   *
   * @param i Index into NPA.
   * @return Value of NPA at specified index.
   */
  @Override public long lookupNPA(long i) {
    long cellValue, rowOff;

    try {
      if (i > getOriginalSize() - 1 || i < 0) {
        throw new ArrayIndexOutOfBoundsException(
          "NPA index out of bounds: i = " + i + " originalSize = " + getOriginalSize());
      }
      int colId, rowId, cellId, cellOff, contextSize, contextPos;
      long colOff;

      // Search columnoffset
      colId = SerializedOperations.ArrayOps.getRank1(coloffsets, 0, getSigmaSize(), i) - 1;

      // Get columnoffset
      colOff = coloffsets.get(colId);

      // Search celloffsets
      cellId = SerializedOperations.ArrayOps
        .getRank1(celloffsets, coff.get(colId), colsizes.get(colId), i - colOff) - 1;

      // Get position within cell
      cellOff = (int) (i - colOff - celloffsets.get(coff.get(colId) + cellId));

      // Search rowoffsets
      rowId = (int) neccol.get(coff.get(colId) + cellId);

      // Get rowoffset
      rowOff = rowoffsets.get(rowId);

      // Get context size
      contextSize = rowsizes.get(rowId);

      // Get context position
      contextPos =
        SerializedOperations.ArrayOps.getRank1(necrow, roff.get(rowId), rowsizes.get(rowId), colId)
          - 1;

      cellValue = cellOff;

      if (wavelettree[rowId] != null) {
        cellValue = wavelettree[rowId].lookup(contextPos, cellOff, 0, contextSize - 1);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return rowOff + cellValue;
  }

  /**
   * Lookup SA at specified index.
   *
   * @param i Index into SA.
   * @return Value of SA at specified index.
   */
  @Override public long lookupSA(long i) {
    long sampledValue, numHops;
    try {
      if (i > getOriginalSize() - 1 || i < 0) {
        throw new ArrayIndexOutOfBoundsException(
          "SA index out of bounds: i = " + i + " originalSize = " + getOriginalSize());
      }

      numHops = 0;
      while (i % getSamplingRate() != 0) {
        i = lookupNPA(i);
        numHops++;
      }
      sampledValue = SerializedOperations.BMArrayOps
        .getVal(sa, (int) (i / getSamplingRate()), getSampledSABits());

      if (sampledValue < numHops) {
        return getOriginalSize() - (numHops - sampledValue);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return sampledValue - numHops;
  }

  /**
   * Lookup ISA at specified index.
   *
   * @param i Index into ISA.
   * @return Value of ISA at specified index.
   */
  @Override public long lookupISA(long i) {
    long pos;
    try {
      if (i > getOriginalSize() - 1 || i < 0) {
        throw new ArrayIndexOutOfBoundsException(
          "ISA index out of bounds: i = " + i + " originalSize = " + getOriginalSize());
      }

      int sampleIdx = (int) (i / getSamplingRate());
      pos = SerializedOperations.BMArrayOps.getVal(isa, sampleIdx, getSampledSABits());
      i -= (sampleIdx * getSamplingRate());
      while (i != 0) {
        pos = lookupNPA(pos);
        i--;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return pos;
  }

  /**
   * Lookup up the inverted alphabet map at specified index.
   *
   * @param i Index into inverted alphabet map
   * @return Value of inverted alphabet map at specified index.
   */
  @Override public byte lookupC(long i) {
    try {
      return alphabet.get(SerializedOperations.ArrayOps.getRank1(coloffsets, 0, getSigmaSize(), i) - 1);
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

  @Override public int getCompressedSize() {
    return (int) endOfCoreStream;
  }

  /**
   * Close all underlying stream.
   */
  void close() throws IOException {
    originalStream.close();
  }
}
