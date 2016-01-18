package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.util.SuccinctConstants;
import edu.berkeley.cs.succinct.util.container.Pair;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.*;

public abstract class SuccinctCore implements Serializable {

  // Logger
  public final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
  static {
    final Handler consoleHandler = new ConsoleHandler();
    consoleHandler.setFormatter(new Formatter() {
      public String format(LogRecord record) {
        return "[" + record.getLevel() + "] " + new Date(record.getMillis()) + " " + record
          .getMessage() + "\n";
      }
    });
    logger.setUseParentHandlers(false);
    logger.addHandler(consoleHandler);
    logger.setLevel(Level.OFF);
  }

  // End of File marker
  public transient static final byte EOF = -127;

  // End of Alphabet marker
  public transient static final byte EOA = -126;

  // End of Line marker
  public transient static final byte EOL = '\n';

  // Metadata
  private transient int originalSize;
  private transient int alphabetSize;
  private transient int samplingRateSA;
  private transient int samplingRateISA;
  private transient int samplingRateNPA;
  private transient int sampleBitWidth;

  // Alphabet map
  protected transient HashMap<Byte, Pair<Integer, Integer>> alphabetMap;
  protected transient byte[] alphabet;

  public SuccinctCore() {
  }

  /**
   * Get the original size.
   *
   * @return The originalSize.
   */
  public int getOriginalSize() {
    return originalSize;
  }

  /**
   * Set the original size.
   *
   * @param originalSize The originalSize to set.
   */
  public void setOriginalSize(int originalSize) {
    this.originalSize = originalSize;
  }

  /**
   * Get the alpha size.
   *
   * @return The alphabetSize.
   */
  public int getAlphabetSize() {
    return alphabetSize;
  }

  /**
   * Set the alpha size.
   *
   * @param alphabetSize The alphabetSize to set.
   */
  public void setAlphabetSize(int alphabetSize) {
    this.alphabetSize = alphabetSize;
  }

  /**
   * Get the SA sampling rate.
   *
   * @return The samplingRate.
   */
  public int getSamplingRateSA() {
    return samplingRateSA;
  }

  /**
   * Set the SA sampling rate.
   *
   * @param samplingRate The samplingRate to set.
   */
  public void setSamplingRateSA(int samplingRate) {
    this.samplingRateSA = samplingRate;
  }

  /**
   * Get the ISA sampling rate.
   *
   * @return The samplingRate.
   */
  public int getSamplingRateISA() {
    return samplingRateISA;
  }

  /**
   * Set the ISA sampling rate.
   *
   * @param samplingRate The samplingRate to set.
   */
  public void setSamplingRateISA(int samplingRate) {
    this.samplingRateISA = samplingRate;
  }

  /**
   * Get the NPA sampling rate.
   *
   * @return The samplingRate.
   */
  public int getSamplingRateNPA() {
    return samplingRateNPA;
  }

  /**
   * Set the NPA sampling rate.
   *
   * @param samplingRate The samplingRate to set.
   */
  public void setSamplingRateNPA(int samplingRate) {
    this.samplingRateNPA = samplingRate;
  }

  /**
   * Get the sample bit width.
   *
   * @return The sample bit width.
   */
  public int getSampleBitWidth() {
    return sampleBitWidth;
  }

  /**
   * Set the sample bit width.
   *
   * @param sampleBitWidth The sample bit width to set.
   */
  public void setSampleBitWidth(int sampleBitWidth) {
    this.sampleBitWidth = sampleBitWidth;
  }

  protected int baseSize() {
    return 6 * SuccinctConstants.INT_SIZE_BYTES // For constants
      + (12 + alphabet.length * SuccinctConstants.BYTE_SIZE_BYTES) // For Byte Array
      + (alphabetMap.size() * 64); // Upper bound for HashMap
  }

  /**
   * Get the size (in bytes) of Succinct data structures (compressed).
   *
   * @return Size (in bytes) of Succinct data structures (compressed).
   */
  public abstract int getSuccinctSize();

  /**
   * Lookup NPA at specified index.
   *
   * @param i Index into NPA.
   * @return Value of NPA at specified index.
   */
  public abstract long lookupNPA(long i);

  /**
   * Lookup SA at specified index.
   *
   * @param i Index into SA.
   * @return Value of SA at specified index.
   */
  public abstract long lookupSA(long i);

  /**
   * Lookup ISA at specified index.
   *
   * @param i Index into ISA.
   * @return Value of ISA at specified index.
   */
  public abstract long lookupISA(long i);

  /**
   * Lookup up the inverted alphabet map at specified index.
   *
   * @param i Index into inverted alphabet map
   * @return Value of inverted alphabet map at specified index.
   */
  public abstract byte lookupC(long i);

  /**
   * Binary Search for a value withing NPA.
   *
   * @param val      Value to be searched.
   * @param startIdx Starting index into NPA.
   * @param endIdx   Ending index into NPA.
   * @param flag     Whether to search for left or the right boundary.
   * @return Search result as an index into the NPA.
   */
  public abstract long binSearchNPA(long val, long startIdx, long endIdx, boolean flag);
}
