package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctCore;
import edu.berkeley.cs.succinct.util.BitUtils;
import edu.berkeley.cs.succinct.util.CommonUtils;
import edu.berkeley.cs.succinct.util.IOUtils;
import edu.berkeley.cs.succinct.util.SuccinctConstants;
import edu.berkeley.cs.succinct.util.buffer.ThreadSafeByteBuffer;
import edu.berkeley.cs.succinct.util.buffer.ThreadSafeLongBuffer;
import edu.berkeley.cs.succinct.util.buffer.serops.ArrayOps;
import edu.berkeley.cs.succinct.util.buffer.serops.DeltaEncodedIntVectorOps;
import edu.berkeley.cs.succinct.util.buffer.serops.IntVectorOps;
import edu.berkeley.cs.succinct.util.container.Pair;
import edu.berkeley.cs.succinct.util.suffixarray.QSufSort;
import edu.berkeley.cs.succinct.util.vector.DeltaEncodedIntVector;
import edu.berkeley.cs.succinct.util.vector.IntVector;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.HashMap;

public class SuccinctBuffer extends SuccinctCore {

  // To maintain versioning
  private static final long serialVersionUID = 1382615274437547247L;

  // Serialized data structures
  protected transient ThreadSafeLongBuffer sa;
  protected transient ThreadSafeLongBuffer isa;
  protected transient ThreadSafeLongBuffer columnoffsets;
  protected transient ThreadSafeByteBuffer[] columns;

  // Storage mode
  protected transient StorageMode storageMode;

  /**
   * Default constructor.
   */
  public SuccinctBuffer() {
    super();
  }

  @Override public int getSuccinctSize() {
    // Compute size of all columns
    int columnsSize = 0;
    for (ThreadSafeByteBuffer column : columns) {
      columnsSize += (12 + column.capacity() * SuccinctConstants.BYTE_SIZE_BYTES);
    }

    return baseSize()
      + (12 + sa.capacity() * SuccinctConstants.LONG_SIZE_BYTES)
      + (12 + isa.capacity() * SuccinctConstants.LONG_SIZE_BYTES)
      + (12 + columnoffsets.capacity() * SuccinctConstants.LONG_SIZE_BYTES)
      + (12 + columns.length * SuccinctConstants.REF_SIZE_BYTES)
      + columnsSize;
  }

  /**
   * Constructor to initialize SuccinctCore from input byte array.
   *
   * @param input Input byte array.
   */
  public SuccinctBuffer(byte[] input) {
    // Construct Succinct data-structures
    construct(input);
  }

  /**
   * Constructor to load the data from persisted Succinct data-structures.
   *
   * @param path        Path to load data from.
   * @param storageMode Mode in which data is stored (In-memory or Memory-mapped)
   */
  public SuccinctBuffer(String path, StorageMode storageMode) {
    this.storageMode = storageMode;
    try {
      if (storageMode == StorageMode.MEMORY_ONLY) {
        readFromFile(path);
      } else if (storageMode == StorageMode.MEMORY_MAPPED) {
        memoryMap(path);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Constructor to load the data from a DataInputStream.
   *
   * @param is Input stream to load the data from
   */
  public SuccinctBuffer(DataInputStream is) {
    try {
      readFromStream(is);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Constructor to load the data from a ByteBuffer.
   *
   * @param buf Input buffer to load the data from
   */
  public SuccinctBuffer(ByteBuffer buf) {
    mapFromBuffer(buf);
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

    int colId = ArrayOps.getRank1(columnoffsets.buffer(), 0, getAlphabetSize(), (int) i) - 1;

    assert colId < getAlphabetSize();
    assert columnoffsets.get(colId) <= i;

    return (long) DeltaEncodedIntVectorOps
      .get(columns[colId].buffer(), (int) (i - columnoffsets.get(colId)));
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

    int j = 0;
    while (i % getSamplingRateSA() != 0) {
      i = lookupNPA(i);
      j++;
    }
    long saVal =
      IntVectorOps.get(sa.buffer(), (int) (i / getSamplingRateSA()), getSampleBitWidth());

    if (saVal < j)
      return getOriginalSize() - (j - saVal);
    return saVal - j;
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

    int sampleIdx = (int) (i / getSamplingRateISA());
    int pos = IntVectorOps.get(isa.buffer(), sampleIdx, getSampleBitWidth());
    i -= (sampleIdx * getSamplingRateISA());
    while (i-- != 0) {
      pos = (int) lookupNPA(pos);
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
    if (i > getOriginalSize() - 1 || i < 0) {
      throw new ArrayIndexOutOfBoundsException(
        "C index out of bounds: i = " + i + " originalSize = " + getOriginalSize());
    }

    int idx = ArrayOps.getRank1(columnoffsets.buffer(), 0, getAlphabetSize(), (int) i) - 1;
    return alphabet[idx];
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

  /**
   * Construct Succinct data structures from input byte array.
   *
   * @param input Input byte array.
   */
  private void construct(byte[] input) {

    // Uncompressed ISA
    int[] ISA;

    logger.info("Constructing Succinct data structures.");
    long startTimeGlobal = System.currentTimeMillis();

    assert IOUtils.checkBytes(input) == -1;

    {
      long startTime = System.currentTimeMillis();

      // Append the EOF byte
      int end = input.length;
      input = Arrays.copyOf(input, input.length + 1);
      input[end] = EOF;

      long timeTaken = (System.currentTimeMillis() - startTime) / 1000L;
      logger.info("Cleaned input in " + timeTaken + "s.");
    }


    // Scope of SA, input
    {
      long startTime = System.currentTimeMillis();

      // Build SA, ISA
      QSufSort suffixSorter = new QSufSort();
      suffixSorter.buildSuffixArray(input);

      int[] SA = suffixSorter.getSA();
      ISA = suffixSorter.getISA();

      // Set metadata
      setOriginalSize(input.length);
      setSamplingRateSA(SuccinctConstants.DEFAULT_SA_SAMPLING_RATE);
      setSamplingRateISA(SuccinctConstants.DEFAULT_ISA_SAMPLING_RATE);
      setSamplingRateNPA(SuccinctConstants.DEFAULT_NPA_SAMPLING_RATE);
      setSampleBitWidth(BitUtils.bitWidth(getOriginalSize()));
      setAlphabetSize(suffixSorter.getAlphabetSize());

      // Get alphabet
      alphabet = suffixSorter.getAlphabet();

      long timeTaken = (System.currentTimeMillis() - startTime) / 1000L;
      logger.info("Built SA, ISA and set metadata in " + timeTaken + "s.");

      startTime = System.currentTimeMillis();

      // Populate column offsets and alphabetMap
      int pos = 0;
      alphabetMap = new HashMap<>();
      alphabetMap.put(input[SA[0]], new Pair<>(0, pos));
      columnoffsets = ThreadSafeLongBuffer.allocate(getAlphabetSize());
      columnoffsets.put(pos, 0);
      pos++;
      for (int i = 1; i < getOriginalSize(); ++i) {
        if (input[SA[i]] != input[SA[i - 1]]) {
          alphabetMap.put(input[SA[i]], new Pair<>(i, pos));
          columnoffsets.put(pos, i);
          pos++;
        }
      }
      alphabetMap.put(SuccinctCore.EOA, new Pair<>(getOriginalSize(), getAlphabetSize()));
      columnoffsets.rewind();

      timeTaken = (System.currentTimeMillis() - startTime) / 1000L;
      logger.info("Computed alphabet map and column offsets in " + timeTaken + "s.");
    }

    // Scope of NPA
    {
      long startTime = System.currentTimeMillis();

      // Construct NPA
      int[] NPA = new int[getOriginalSize()];
      for (int i = 1; i < getOriginalSize(); i++) {
        NPA[ISA[i - 1]] = ISA[i];
      }
      NPA[ISA[getOriginalSize() - 1]] = ISA[0];

      long timeTaken = (System.currentTimeMillis() - startTime) / 1000L;
      logger.info("Built uncompressed NPA in " + timeTaken + "s.");

      startTime = System.currentTimeMillis();

      // Compress NPA
      logger.info("Compressing NPA in " + getAlphabetSize() + " columns...");
      columns = new ThreadSafeByteBuffer[getAlphabetSize()];
      for (int i = 0; i < getAlphabetSize(); i++) {
        int startOffset = (int) columnoffsets.get(i);
        int endOffset =
          (i < getAlphabetSize() - 1) ? (int) columnoffsets.get(i + 1) : getOriginalSize();
        int length = endOffset - startOffset;
        DeltaEncodedIntVector columnVector =
          new DeltaEncodedIntVector(NPA, startOffset, length, getSamplingRateNPA());
        int columnSizeInBytes = columnVector.serializedSize();
        columns[i] = ThreadSafeByteBuffer.allocate(columnSizeInBytes);
        columnVector.writeToBuffer(columns[i].buffer());
        columns[i].rewind();
        logger.info("Compressed column " + i + ".");
      }

      timeTaken = (System.currentTimeMillis() - startTime) / 1000L;
      logger.info("Compressed NPA in " + timeTaken + "s.");
    }

    {
      long startTime = System.currentTimeMillis();

      // Sample SA, ISA
      IntVector sampledSA, sampledISA;
      int numSampledElementsSA = CommonUtils.numBlocks(getOriginalSize(), getSamplingRateSA());
      int numSampledElementsISA = CommonUtils.numBlocks(getOriginalSize(), getSamplingRateISA());
      int sampleBitWidth = BitUtils.bitWidth(getOriginalSize());
      sampledSA = new IntVector(numSampledElementsSA, sampleBitWidth);
      sampledISA = new IntVector(numSampledElementsISA, sampleBitWidth);
      for (int val = 0; val < getOriginalSize(); val++) {
        int idx = ISA[val];
        if (idx % getSamplingRateSA() == 0) {
          sampledSA.add(idx / getSamplingRateSA(), val);
        }
        if (val % getSamplingRateISA() == 0) {
          sampledISA.add(val / getSamplingRateISA(), idx);
        }
      }
      sa = ThreadSafeLongBuffer.wrap(sampledSA.getData());
      sa.rewind();
      isa = ThreadSafeLongBuffer.wrap(sampledISA.getData());
      isa.rewind();

      long timeTaken = (System.currentTimeMillis() - startTime) / 1000L;
      logger.info("Sampled SA, ISA in " + timeTaken + "s.");
    }

    long timeTakenGlobal = (System.currentTimeMillis() - startTimeGlobal) / 1000L;
    logger.info("Finished constructing Succinct data structures in " + timeTakenGlobal + "s.");
  }

  /**
   * Write Succinct data structures to a DataOutputStream.
   *
   * @param os Output stream to write data to.
   * @throws IOException
   */
  public void writeToStream(DataOutputStream os) throws IOException {
    WritableByteChannel dataChannel = Channels.newChannel(os);

    os.writeInt(getOriginalSize());
    os.writeInt(getSamplingRateSA());
    os.writeInt(getSamplingRateISA());
    os.writeInt(getSamplingRateNPA());
    os.writeInt(getSampleBitWidth());
    os.writeInt(getAlphabetSize());

    for (Byte c : alphabetMap.keySet()) {
      Pair<Integer, Integer> cval = alphabetMap.get(c);
      os.write(c);
      os.writeInt(cval.first);
      os.writeInt(cval.second);
    }

    os.write(alphabet);

    ByteBuffer bufSA = ByteBuffer.allocate(sa.limit() * SuccinctConstants.LONG_SIZE_BYTES);
    bufSA.asLongBuffer().put(sa.buffer());
    dataChannel.write(bufSA.order(ByteOrder.BIG_ENDIAN));
    sa.rewind();

    ByteBuffer bufISA = ByteBuffer.allocate(isa.limit() * SuccinctConstants.LONG_SIZE_BYTES);
    bufISA.asLongBuffer().put(isa.buffer());
    dataChannel.write(bufISA.order(ByteOrder.BIG_ENDIAN));
    isa.rewind();

    ByteBuffer bufColOff =
      ByteBuffer.allocate(getAlphabetSize() * SuccinctConstants.LONG_SIZE_BYTES);
    bufColOff.asLongBuffer().put(columnoffsets.buffer());
    dataChannel.write(bufColOff.order(ByteOrder.BIG_ENDIAN));
    columnoffsets.rewind();

    for (int i = 0; i < columns.length; i++) {
      os.writeInt(columns[i].limit());
      dataChannel.write(columns[i].order(ByteOrder.BIG_ENDIAN));
      columns[i].rewind();
    }
  }

  /**
   * Reads Succinct data structures from a DataInputStream.
   *
   * @param is Stream to read data structures from.
   * @throws IOException
   */
  public void readFromStream(DataInputStream is) throws IOException {
    ReadableByteChannel dataChannel = Channels.newChannel(is);
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

    // Read sa
    ByteBuffer saBuf = ByteBuffer
      .allocate(BitUtils.bitsToBlocks64(totalSampledBitsSA) * SuccinctConstants.LONG_SIZE_BYTES);
    dataChannel.read(saBuf);
    saBuf.rewind();
    sa = ThreadSafeLongBuffer.fromLongBuffer(saBuf.asLongBuffer());

    // Compute number of sampled elements
    int totalSampledBitsISA =
      CommonUtils.numBlocks(getOriginalSize(), getSamplingRateISA()) * getSampleBitWidth();

    // Read isa
    ByteBuffer isaBuf = ByteBuffer
      .allocate(BitUtils.bitsToBlocks64(totalSampledBitsISA) * SuccinctConstants.LONG_SIZE_BYTES);
    dataChannel.read(isaBuf);
    isaBuf.rewind();
    isa = ThreadSafeLongBuffer.fromLongBuffer(isaBuf.asLongBuffer());

    // Read columnoffsets
    ByteBuffer coloffsetsBuf =
      ByteBuffer.allocate(getAlphabetSize() * SuccinctConstants.LONG_SIZE_BYTES);
    dataChannel.read(coloffsetsBuf);
    coloffsetsBuf.rewind();
    columnoffsets = ThreadSafeLongBuffer.fromLongBuffer(coloffsetsBuf.asLongBuffer());

    // Read NPA columns
    columns = new ThreadSafeByteBuffer[getAlphabetSize()];
    for (int i = 0; i < getAlphabetSize(); i++) {
      int columnSize = is.readInt();
      ByteBuffer columnBuf = ByteBuffer.allocate(columnSize);
      dataChannel.read(columnBuf);
      columns[i] = ThreadSafeByteBuffer.fromByteBuffer(((ByteBuffer) columnBuf.rewind()));
    }
  }

  /**
   * Slices, orders and limits ByteBuffer.
   *
   * @param buf  Buffer to slice, order and limit.
   * @param size Size to which buffer should be limited.
   * @return Sliced, ordered and limited buffer.
   */
  private ByteBuffer sliceOrderLimit(ByteBuffer buf, int size) {
    ByteBuffer ret = (ByteBuffer) buf.slice().order(ByteOrder.BIG_ENDIAN).limit(size);
    buf.position(buf.position() + size);
    return ret;
  }

  /**
   * Reads Succinct data structures from a ByteBuffer.
   *
   * @param buf ByteBuffer to read Succinct data structures from.
   */
  public void mapFromBuffer(ByteBuffer buf) {
    buf.rewind();

    // Deserialize metadata
    setOriginalSize(buf.getInt());
    setSamplingRateSA(buf.getInt());
    setSamplingRateISA(buf.getInt());
    setSamplingRateNPA(buf.getInt());
    setSampleBitWidth(buf.getInt());
    setAlphabetSize(buf.getInt());

    // Deserialize alphabet map
    alphabetMap = new HashMap<>();
    for (int i = 0; i < getAlphabetSize() + 1; i++) {
      byte c = buf.get();
      int v1 = buf.getInt();
      int v2 = buf.getInt();
      alphabetMap.put(c, new Pair<>(v1, v2));
    }

    // Read alphabet
    alphabet = new byte[getAlphabetSize()];
    buf.get(alphabet);

    // Compute number of sampled elements
    int totalSampledBitsSA =
      CommonUtils.numBlocks(getOriginalSize(), getSamplingRateSA()) * getSampleBitWidth();

    // Read sa
    int saSize = BitUtils.bitsToBlocks64(totalSampledBitsSA) * SuccinctConstants.LONG_SIZE_BYTES;
    sa = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, saSize).asLongBuffer());

    // Compute number of sampled elements
    int totalSampledBitsISA =
      CommonUtils.numBlocks(getOriginalSize(), getSamplingRateISA()) * getSampleBitWidth();

    // Read isa
    int isaSize = BitUtils.bitsToBlocks64(totalSampledBitsISA) * SuccinctConstants.LONG_SIZE_BYTES;
    isa = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, isaSize).asLongBuffer());

    // Read columnoffsets
    int coloffsetsSize = getAlphabetSize() * SuccinctConstants.LONG_SIZE_BYTES;
    columnoffsets =
      ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, coloffsetsSize).asLongBuffer());

    columns = new ThreadSafeByteBuffer[getAlphabetSize()];
    for (int i = 0; i < getAlphabetSize(); i++) {
      int columnSize = buf.getInt();
      columns[i] = ThreadSafeByteBuffer.fromByteBuffer(sliceOrderLimit(buf, columnSize));
      columns[i].rewind();
    }
  }

  /**
   * Write Succinct data structures to file.
   *
   * @param path Path to file where Succinct data structures should be written.
   * @throws IOException
   */
  public void writeToFile(String path) throws IOException {
    FileOutputStream fos = new FileOutputStream(path);
    DataOutputStream os = new DataOutputStream(fos);
    writeToStream(os);
  }

  /**
   * Read Succinct data structures into memory from file.
   *
   * @param path Path to serialized Succinct data structures.
   * @throws IOException
   */
  public void readFromFile(String path) throws IOException {
    FileInputStream fis = new FileInputStream(path);
    DataInputStream is = new DataInputStream(fis);
    readFromStream(is);
  }

  /**
   * Memory maps serialized Succinct data structures.
   *
   * @param path Path to serialized Succinct data structures.
   * @throws IOException
   */
  public void memoryMap(String path) throws IOException {
    File file = new File(path);
    long size = file.length();
    FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();

    ByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
    mapFromBuffer(buf);
  }

  /**
   * Serialize SuccinctBuffer to OutputStream.
   *
   * @param oos ObjectOutputStream to write to.
   * @throws IOException
   */
  private void writeObject(ObjectOutputStream oos) throws IOException {
    writeToStream(new DataOutputStream(oos));
  }

  /**
   * Deserialize SuccinctBuffer from InputStream.
   *
   * @param ois ObjectInputStream to read from.
   * @throws IOException
   */
  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    readFromStream(new DataInputStream(ois));
  }

}
