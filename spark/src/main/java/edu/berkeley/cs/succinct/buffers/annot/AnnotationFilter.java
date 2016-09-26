package edu.berkeley.cs.succinct.buffers.annot;

public interface AnnotationFilter {
  boolean metadataFilter(String metadata);
  boolean textFilter(String docId, int startOffset, int endOffset);
}
