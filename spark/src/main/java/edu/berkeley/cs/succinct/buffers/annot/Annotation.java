package edu.berkeley.cs.succinct.buffers.annot;

import java.io.Serializable;

public class Annotation implements Serializable {
  private String docId;
  private int id;
  private int startOffset;
  private int endOffset;
  private String metadata;

  public Annotation(String docId, int id, int rBegin, int rEnd, String metadata) {
    this.docId = docId;
    this.id = id;
    this.startOffset = rBegin;
    this.endOffset = rEnd;
    this.metadata = metadata;
  }

  /**
   * Get the document ID for the annotation.
   *
   * @return The document ID for the annotation.
   */
  public String getDocId() {
    return docId;
  }

  /**
   * Get the annotation ID for the annotation.
   *
   * @return The annotation ID for the annotation.
   */
  public int getId() {
    return id;
  }

  /**
   * Get the start offset for the annotation.
   *
   * @return The start offset for the annotation.
   */
  public int getStartOffset() {
    return startOffset;
  }

  /**
   * Get the end offset for the annotation.
   *
   * @return The end offset for the annotation.
   */
  public int getEndOffset() {
    return endOffset;
  }

  /**
   * Get the metadata for the annotation.
   *
   * @return The metadata for the annotation.
   */
  public String getMetadata() {
    return metadata;
  }

  public String toString() {
    return "[" + docId + ", " + id + ", " + startOffset + ", " + endOffset + ", " + metadata + "]";
  }
}
