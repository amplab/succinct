package edu.berkeley.cs.succinct.buffers.annot;

import java.io.Serializable;

public class Annotation implements Serializable {
  private String docId;
  private int id;
  private String aClass;
  private String aType;
  private int rBegin;
  private int rEnd;
  private String metadata;

  public Annotation(String docId, int id, String aClass, String aType, int rBegin, int rEnd, String metadata) {
    this.docId = docId;
    this.id = id;
    this.aClass = aClass;
    this.aType = aType;
    this.rBegin = rBegin;
    this.rEnd = rEnd;
    this.metadata = metadata;
  }

  public String getDocId() {
    return docId;
  }

  public void setDocId(String docId) {
    this.docId = docId;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getaClass() {
    return aClass;
  }

  public void setaClass(String aClass) {
    this.aClass = aClass;
  }

  public String getaType() {
    return aType;
  }

  public void setaType(String aType) {
    this.aType = aType;
  }

  public int getrBegin() {
    return rBegin;
  }

  public void setrBegin(int rBegin) {
    this.rBegin = rBegin;
  }

  public int getrEnd() {
    return rEnd;
  }

  public void setrEnd(int rEnd) {
    this.rEnd = rEnd;
  }

  public String getMetadata() {
    return metadata;
  }

  public void setMetadata(String metadata) {
    this.metadata = metadata;
  }
}
