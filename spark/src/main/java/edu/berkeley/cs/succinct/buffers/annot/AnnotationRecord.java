package edu.berkeley.cs.succinct.buffers.annot;

public class AnnotationRecord {
  private int offset;
  private String docId;
  private String annotClass;
  private String annotType;
  private int numEntries;

  public AnnotationRecord(int offset, String docId, String annotClass, String annotType,
    int numEntries) {
    this.offset = offset;
    this.docId = docId;
    this.annotClass = annotClass;
    this.annotType = annotType;
    this.numEntries = numEntries;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public String getDocId() {
    return docId;
  }

  public void setDocId(String docId) {
    this.docId = docId;
  }

  public String getAnnotClass() {
    return annotClass;
  }

  public void setAnnotClass(String annotClass) {
    this.annotClass = annotClass;
  }

  public String getAnnotType() {
    return annotType;
  }

  public void setAnnotType(String annotType) {
    this.annotType = annotType;
  }

  public int getNumEntries() {
    return numEntries;
  }

  public void setNumEntries(int numEntries) {
    this.numEntries = numEntries;
  }
}
