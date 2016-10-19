package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctCore;
import edu.berkeley.cs.succinct.util.BitUtils;
import edu.berkeley.cs.succinct.util.CommonUtils;
import edu.berkeley.cs.succinct.util.Source;
import edu.berkeley.cs.succinct.util.SuccinctConstants;
import edu.berkeley.cs.succinct.util.buffer.ThreadSafeByteBuffer;
import edu.berkeley.cs.succinct.util.buffer.ThreadSafeLongBuffer;
import edu.berkeley.cs.succinct.util.buffer.serops.ArrayOps;
import edu.berkeley.cs.succinct.util.buffer.serops.DeltaEncodedIntVectorOps;
import edu.berkeley.cs.succinct.util.buffer.serops.IntVectorOps;
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

  // Succinct core data structures buffer
  protected transient ByteBuffer core;

  /**
   * Default constructor.
   */
  public SuccinctBuffer() {
    super();
  }

  /**
   * Constructor to initialize SuccinctCore from input byte array.
   *
   * @param input Input byte array.
   */
  public SuccinctBuffer(final byte[] input) {
    // Construct Succinct data-structures
    try {
      construct(new Source() {
        @Override public int length() {
          return input.length;
        }

        @Override public int get(int i) {
          return input[i];
        }
      });
    } catch (IOException e) {
      throw new RuntimeException("Could not create core data structures", e);
    }
    mapFromCore();
  }

  /**
   * Constructor to initialize SuccinctCore from input char array.
   *
   * @param input Input char array.
   */
  public SuccinctBuffer(final char[] input) {
    // Construct Succinct data-structures
    try {
      construct(new Source() {
        @Override public int length() {
          return input.length;
        }

        @Override public int get(int i) {
          return input[i];
        }
      });
    } catch (IOException e) {
      throw new RuntimeException("Could not create core data structures", e);
    }
    mapFromCore();
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
    this.core = buf;
    mapFromCore();
  }

  @Override public int getCoreSize() {
    return core.limit();
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
  @Override public int lookupC(long i) {
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

    if (endIdx < startIdx)
      return endIdx;

    int colId = ArrayOps.getRank1(columnoffsets.buffer(), 0, getAlphabetSize(), (int) startIdx) - 1;
    long colValue = columnoffsets.get(colId);

    int sp = (int) (startIdx - colValue);
    int ep = (int) (endIdx - colValue);

    int res =
      DeltaEncodedIntVectorOps.binarySearch(columns[colId].buffer(), (int) val, sp, ep, flag);

    return colValue + res;
  }

  /**
   * Construct Succinct data structures from input byte array.
   *
   * @param input Input byte array.
   */
  protected void construct(Source input) throws IOException {

    // Uncompressed ISA
    int[] ISA;
    int[] columnOffsets;

    LOG.info("Constructing Succinct data structures.");
    long startTimeGlobal = System.currentTimeMillis();

    File tmpFile = File.createTempFile("succinct-construct-", ".tmp");
    tmpFile.deleteOnExit();
    DataOutputStream coreStream = new DataOutputStream(new FileOutputStream(tmpFile));

    int originalSize = input.length() + 1;

    int samplingRateSA = SuccinctConstants.DEFAULT_SA_SAMPLING_RATE;
    int samplingRateISA = SuccinctConstants.DEFAULT_ISA_SAMPLING_RATE;
    int samplingRateNPA = SuccinctConstants.DEFAULT_NPA_SAMPLING_RATE;
    int sampleBitWidth = BitUtils.bitWidth(input.length() + 1);
    int alphabetSize;

    // Scope of SA, input
    {
      long startTime = System.currentTimeMillis();

      // Build SA, ISA
      QSufSort suffixSorter = new QSufSort();
      suffixSorter.buildSuffixArray(input);

      int[] SA = suffixSorter.getSA();
      ISA = suffixSorter.getISA();
      alphabetSize = suffixSorter.getAlphabetSize();

      // Set metadata
      coreStream.writeInt(originalSize);    // Original size
      coreStream.writeInt(samplingRateSA);  // SA sampling rate
      coreStream.writeInt(samplingRateISA); // ISA sampling rate
      coreStream.writeInt(samplingRateNPA); // NPA sampling rate
      coreStream.writeInt(sampleBitWidth);  // Sample Width
      coreStream.writeInt(alphabetSize);    // Alphabet size

      // Get alphabet
      int[] alphabetArray = suffixSorter.getAlphabet();
      for (int i = 0; i < alphabetSize; i++) {
        coreStream.writeInt(alphabetArray[i]);
      }

      long timeTaken = (System.currentTimeMillis() - startTime) / 1000L;
      LOG.info("Built SA, ISA and set metadata in " + timeTaken + "s.");

      // Save column offsets in an array
      startTime = System.currentTimeMillis();

      int pos = 0;
      int prevSortedChar = SuccinctConstants.EOF;
      columnOffsets = new int[alphabetSize];
      columnOffsets[pos] = 0;
      pos++;
      for (int i = 1; i < originalSize; ++i) {
        if (input.get(SA[i]) != prevSortedChar) {
          prevSortedChar = input.get(SA[i]);
          columnOffsets[pos] = i;
          pos++;
        }
      }

      timeTaken = (System.currentTimeMillis() - startTime) / 1000L;
      LOG.info("Computed column offsets in " + timeTaken + "s.");
    }

    {
      // Sample SA, ISA
      long startTime = System.currentTimeMillis();

      IntVector sampledSA, sampledISA;
      int numSampledElementsSA = CommonUtils.numBlocks(originalSize, samplingRateSA);
      int numSampledElementsISA = CommonUtils.numBlocks(originalSize, samplingRateISA);
      sampledSA = new IntVector(numSampledElementsSA, sampleBitWidth);
      sampledISA = new IntVector(numSampledElementsISA, sampleBitWidth);
      for (int val = 0; val < originalSize; val++) {
        int idx = ISA[val];
        if (idx % samplingRateSA == 0) {
          sampledSA.add(idx / samplingRateSA, val);
        }
        if (val % samplingRateISA == 0) {
          sampledISA.add(val / samplingRateISA, idx);
        }
      }
      sampledSA.writeDataToStream(coreStream);
      sampledISA.writeDataToStream(coreStream);

      long timeTaken = (System.currentTimeMillis() - startTime) / 1000L;
      LOG.info("Sampled SA, ISA in " + timeTaken + "s.");
    }

    // Scope of NPA
    {
      long startTime = System.currentTimeMillis();

      // Write column offsets
      for (int i = 0; i < alphabetSize; i++) {
        coreStream.writeLong(columnOffsets[i]);
      }

      // Construct NPA
      int[] NPA = new int[originalSize];
      for (int i = 1; i < originalSize; i++) {
        NPA[ISA[i - 1]] = ISA[i];
      }
      NPA[ISA[originalSize - 1]] = ISA[0];

      long timeTaken = (System.currentTimeMillis() - startTime) / 1000L;
      LOG.info("Built uncompressed NPA in " + timeTaken + "s.");

      startTime = System.currentTimeMillis();

      // Compress NPA
      LOG.info("Compressing NPA in " + alphabetSize + " columns...");
      for (int i = 0; i < alphabetSize; i++) {
        int startOffset = columnOffsets[i];
        int endOffset = (i < alphabetSize - 1) ? columnOffsets[i + 1] : originalSize;
        int length = endOffset - startOffset;
        DeltaEncodedIntVector columnVector =
          new DeltaEncodedIntVector(NPA, startOffset, length, samplingRateNPA);
        coreStream.writeInt(columnVector.serializedSize());
        columnVector.writeToStream(coreStream);
        LOG.info("Compressed column " + i + ".");
      }

      timeTaken = (System.currentTimeMillis() - startTime) / 1000L;
      LOG.info("Compressed NPA in " + timeTaken + "s.");
    }

    long timeTakenGlobal = (System.currentTimeMillis() - startTimeGlobal) / 1000L;
    LOG.info("Finished constructing Succinct data structures in " + timeTakenGlobal + "s.");

    coreStream.close();

    core = ByteBuffer.allocateDirect(((int) tmpFile.length()));
    FileChannel inChannel = new FileInputStream(tmpFile).getChannel();
    inChannel.read(core);
    inChannel.close();

    if (!tmpFile.delete()) {
      LOG.warning("Could not delete temporary file.");
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
   * Reads Succinct data structures from the data ByteBuffer.
   */
  public void mapFromCore() {
    core.order(ByteOrder.BIG_ENDIAN);
    core.rewind();

    // Deserialize metadata
    setOriginalSize(core.getInt());
    setSamplingRateSA(core.getInt());
    setSamplingRateISA(core.getInt());
    setSamplingRateNPA(core.getInt());
    setSampleBitWidth(core.getInt());
    setAlphabetSize(core.getInt());

    // Read alphabet
    alphabet = new int[getAlphabetSize()];
    for (int i = 0; i < getAlphabetSize(); i++) {
      alphabet[i] = core.getInt();
    }

    // Compute number of sampled elements
    int totalSampledBitsSA =
      CommonUtils.numBlocks(getOriginalSize(), getSamplingRateSA()) * getSampleBitWidth();

    // Read sa
    int saSize = BitUtils.bitsToBlocks64(totalSampledBitsSA) * SuccinctConstants.LONG_SIZE_BYTES;
    sa = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(core, saSize).asLongBuffer());

    // Compute number of sampled elements
    int totalSampledBitsISA =
      CommonUtils.numBlocks(getOriginalSize(), getSamplingRateISA()) * getSampleBitWidth();

    // Read isa
    int isaSize = BitUtils.bitsToBlocks64(totalSampledBitsISA) * SuccinctConstants.LONG_SIZE_BYTES;
    isa = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(core, isaSize).asLongBuffer());

    // Read columnoffsets
    int coloffsetsSize = getAlphabetSize() * SuccinctConstants.LONG_SIZE_BYTES;
    columnoffsets =
      ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(core, coloffsetsSize).asLongBuffer());

    columns = new ThreadSafeByteBuffer[getAlphabetSize()];
    for (int i = 0; i < getAlphabetSize(); i++) {
      int columnSize = core.getInt();
      columns[i] = ThreadSafeByteBuffer.fromByteBuffer(sliceOrderLimit(core, columnSize));
      columns[i].rewind();
    }

    core.rewind();
  }

  /**
   * Write Succinct data structures to a DataOutputStream.
   *
   * @param os Output stream to write data to.
   * @throws IOException
   */
  public void writeToStream(DataOutputStream os) throws IOException {
    WritableByteChannel dataChannel = Channels.newChannel(os);

    os.writeInt(core.limit());
    dataChannel.write(core.order(ByteOrder.BIG_ENDIAN));
    core.rewind();
  }

  /**
   * Reads Succinct data structures from a DataInputStream.
   *
   * @param is Stream to read data structures from.
   * @throws IOException
   */
  public void readFromStream(DataInputStream is) throws IOException {
    ReadableByteChannel dataChannel = Channels.newChannel(is);

    int dataSize = is.readInt();
    core = ByteBuffer.allocateDirect(dataSize);
    dataChannel.read(core);
    mapFromCore();
    core.rewind();
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
    int compressedSize = buf.getInt();
    core = (ByteBuffer) buf.slice().limit(compressedSize);
    mapFromCore();
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
