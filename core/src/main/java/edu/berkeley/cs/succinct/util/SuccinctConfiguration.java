package edu.berkeley.cs.succinct.util;

public class SuccinctConfiguration {

  private int saSamplingRate;
  private int isaSamplingRate;
  private int npaSamplingRate;

  public SuccinctConfiguration(int saSamplingRate, int isaSamplingRate, int npaSamplingRate) {
    this.saSamplingRate = saSamplingRate;
    this.isaSamplingRate = isaSamplingRate;
    this.npaSamplingRate = npaSamplingRate;
  }

  public SuccinctConfiguration() {
    this(SuccinctConstants.DEFAULT_SA_SAMPLING_RATE, SuccinctConstants.DEFAULT_ISA_SAMPLING_RATE,
      SuccinctConstants.DEFAULT_NPA_SAMPLING_RATE);
  }

  public int getSaSamplingRate() {
    return saSamplingRate;
  }

  public void setSaSamplingRate(int saSamplingRate) {
    this.saSamplingRate = saSamplingRate;
  }

  public int getIsaSamplingRate() {
    return isaSamplingRate;
  }

  public void setIsaSamplingRate(int isaSamplingRate) {
    this.isaSamplingRate = isaSamplingRate;
  }

  public int getNpaSamplingRate() {
    return npaSamplingRate;
  }

  public void setNpaSamplingRate(int npaSamplingRate) {
    this.npaSamplingRate = npaSamplingRate;
  }
}
