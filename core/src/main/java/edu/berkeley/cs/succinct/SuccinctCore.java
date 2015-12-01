package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.util.container.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class SuccinctCore implements Serializable {

  // End of File marker
  public transient static final byte EOF = -127;

  // END of Alphabet marker
  public transient static final byte EOA = -126;

  // End of Line marker
  public transient static final byte EOL = '\n';

  // Deserialized data-structures
  protected transient HashMap<Byte, Pair<Long, Integer>> alphabetMap;
  protected transient Map<Long, Long> contextMap;

  // Metadata
  private transient int originalSize;
  private transient int sampledSASize;
  private transient int alphaSize;
  private transient int sigmaSize;
  private transient int bits;
  private transient int sampledSABits;
  private transient int samplingBase;
  private transient int samplingRate;
  private transient int numContexts;
  private transient int contextLen;

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
   * Get the sampled SA size.
   *
   * @return The sampledSASize.
   */
  public int getSampledSASize() {
    return sampledSASize;
  }

  /**
   * Set the sampled SA size.
   *
   * @param sampledSASize The sampledSASize to set.
   */
  public void setSampledSASize(int sampledSASize) {
    this.sampledSASize = sampledSASize;
  }

  /**
   * Get the alpha size.
   *
   * @return The alphaSize.
   */
  public int getAlphaSize() {
    return alphaSize;
  }

  /**
   * Set the alpha size.
   *
   * @param alphaSize The alphaSize to set.
   */
  public void setAlphaSize(int alphaSize) {
    this.alphaSize = alphaSize;
  }

  /**
   * Get the sigma size.
   *
   * @return The sigmaSize.
   */
  public int getSigmaSize() {
    return sigmaSize;
  }

  /**
   * Set the sigma size.
   *
   * @param sigmaSize The sigmaSize to set.
   */
  public void setSigmaSize(int sigmaSize) {
    this.sigmaSize = sigmaSize;
  }

  /**
   * Get the bits.
   *
   * @return The bits.
   */
  public int getBits() {
    return bits;
  }

  /**
   * Set the bits.
   *
   * @param bits The bits to set.
   */
  public void setBits(int bits) {
    this.bits = bits;
  }

  /**
   * Get the sampled SA bits.
   *
   * @return The sampledSABits.
   */
  public int getSampledSABits() {
    return sampledSABits;
  }

  /**
   * Set the sampled SA bits.
   *
   * @param sampledSABits The sampledSABits to set.
   */
  public void setSampledSABits(int sampledSABits) {
    this.sampledSABits = sampledSABits;
  }

  /**
   * Get the sampling base.
   *
   * @return The samplingBase.
   */
  public int getSamplingBase() {
    return samplingBase;
  }

  /**
   * Set the sampling base.
   *
   * @param samplingBase The samplingBase to set.
   */
  public void setSamplingBase(int samplingBase) {
    this.samplingBase = samplingBase;
  }

  /**
   * Get the sampling rate.
   *
   * @return The samplingRate.
   */
  public int getSamplingRate() {
    return samplingRate;
  }

  /**
   * Set the sampling rate.
   *
   * @param samplingRate The samplingRate to set.
   */
  public void setSamplingRate(int samplingRate) {
    this.samplingRate = samplingRate;
  }

  /**
   * Get the number of contexts.
   *
   * @return The numContexts.
   */
  public int getNumContexts() {
    return numContexts;
  }

  /**
   * Set the number of contexts.
   *
   * @param numContexts The numContexts to set.
   */
  public void setNumContexts(int numContexts) {
    this.numContexts = numContexts;
  }

  /**
   * Get the context length.
   *
   * @return The contextLength.
   */
  public int getContextLen() {
    return contextLen;
  }

  /**
   * Set the context length.
   *
   * @param contextLen The contextLen to set.
   */
  public void setContextLen(int contextLen) {
    this.contextLen = contextLen;
  }

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

  /**
   * Get the compressed size for the Succinct encoded data structures.
   *
   * @return The total size in bytes for the Succinct encoded data structures.
   */
  public abstract int getCompressedSize();

}
