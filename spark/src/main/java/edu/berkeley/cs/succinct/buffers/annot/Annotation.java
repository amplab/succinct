package edu.berkeley.cs.succinct.buffers.annot;

import java.io.Serializable;

public class Annotation implements Serializable {
  private String docId;
  private int id;
  private int rBegin;
  private int rEnd;
  private String metadata;

  public Annotation(String docId, int id, int rBegin, int rEnd, String metadata) {
    this.docId = docId;
    this.id = id;
    this.rBegin = rBegin;
    this.rEnd = rEnd;
    this.metadata = metadata;
  }

  public String getDocId() {
    return docId;
  }

  public int getId() {
    return id;
  }

  public int getrBegin() {
    return rBegin;
  }

  public int getrEnd() {
    return rEnd;
  }

  public String getMetadata() {
    return metadata;
  }

  public String toString() {
    return "[" + docId + ", " + id + ", " + rBegin + ", " + rEnd + ", " + metadata + "]";
  }
}
