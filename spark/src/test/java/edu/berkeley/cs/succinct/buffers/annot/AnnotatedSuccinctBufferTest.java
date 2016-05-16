package edu.berkeley.cs.succinct.buffers.annot;

import junit.framework.TestCase;

public class AnnotatedSuccinctBufferTest extends TestCase {

  // See AnnotatedDocumentSerializerSuite.scala for the original annotations.
  private final byte[] test =
    {94, 103, 101, 94, 119, 111, 114, 100, 94, 100, 111, 99, 49, 94, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0,
      0, 9, 0, 0, 0, 16, 0, 0, 0, 8, 0, 0, 0, 15, 0, 0, 0, 19, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 5,
      0, 3, 102, 111, 111, 0, 3, 98, 97, 114, 0, 3, 98, 97, 122, 94, 103, 101, 94, 115, 112, 97, 99,
      101, 94, 100, 111, 99, 49, 94, 0, 0, 0, 2, 0, 0, 0, 8, 0, 0, 0, 15, 0, 0, 0, 9, 0, 0, 0, 16,
      0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 0, 94, 103, 101, 94, 119, 111, 114, 100, 94, 100, 111, 99,
      50, 94, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 16, 0, 0, 0, 8, 0, 0, 0, 15, 0, 0, 0, 19,
      0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 94, 103, 101, 94, 115, 112, 97, 99, 101,
      94, 100, 111, 99, 50, 94, 0, 0, 0, 2, 0, 0, 0, 8, 0, 0, 0, 15, 0, 0, 0, 9, 0, 0, 0, 16, 0, 0,
      0, 2, 0, 0, 0, 4, 0, 0, 0, 0, 94, 103, 101, 94, 115, 112, 97, 99, 101, 94, 100, 111, 99, 51,
      94, 0, 0, 0, 2, 0, 0, 0, 8, 0, 0, 0, 15, 0, 0, 0, 9, 0, 0, 0, 16, 0, 0, 0, 2, 0, 0, 0, 4, 0,
      0, 0, 0, 94, 103, 101, 94, 119, 111, 114, 100, 94, 100, 111, 99, 51, 94, 0, 0, 0, 3, 0, 0, 0,
      0, 0, 0, 0, 9, 0, 0, 0, 16, 0, 0, 0, 8, 0, 0, 0, 15, 0, 0, 0, 21, 0, 0, 0, 1, 0, 0, 0, 3, 0,
      0, 0, 5, 0, 1, 97, 0, 3, 98, 38, 99, 0, 3, 100, 94, 101};

  private final AnnotatedSuccinctBuffer buf = new AnnotatedSuccinctBuffer(test);
  private final String[] documentIds = {"doc1", "doc2", "doc3"};
  private final String[] annotClasses = {"ge"};
  private final String[] annotTypes = {"word", "space"};

  private final int[] rBeginsWord = {0, 9, 16};
  private final int[] rEndsWord = {8, 15, 19};
  private final int[] aIdsWord = {1, 3, 5};

  private final int[] rBeginsSpace = {8, 15};
  private final int[] rEndsSpace = {9, 16};
  private final int[] aIdsSpace = {2, 4};

  private final String[] metadataDoc1Words = {"foo", "bar", "baz"};
  private final String[] metadataDoc3Words = {"a", "b&c", "d^e"};

  public void testFindAnnotationRecord() throws Exception {
    for (String docId : documentIds) {
      for (String annotClass : annotClasses) {
        for (String annotType : annotTypes) {
          AnnotationRecord ar = buf.getAnnotationRecord(docId, annotClass, annotType);
          assertNotNull(ar);
          assertEquals(docId, ar.getDocId());
          assertEquals(annotClass, ar.getAnnotClass());
          assertEquals(annotType, ar.getAnnotType());
          if (annotType.compareTo("word") == 0) {
            assertEquals(3, ar.getNumEntries());
          } else if (annotType.compareTo("space") == 0) {
            assertEquals(2, ar.getNumEntries());
          }
        }
      }
    }

    assertNull(buf.getAnnotationRecord("a", "b", "c"));
  }

  public void testGetRangeBegin() throws Exception {
    for (String docId : documentIds) {
      for (String annotClass : annotClasses) {
        for (String annotType : annotTypes) {
          AnnotationRecord ar = buf.getAnnotationRecord(docId, annotClass, annotType);
          for (int i = 0; i < ar.getNumEntries(); i++) {
            int rBegin = buf.getRangeBegin(ar, i);
            if (annotType.compareTo("word") == 0) {
              assertEquals(rBeginsWord[i], rBegin);
            } else if (annotType.compareTo("space") == 0) {
              assertEquals(rBeginsSpace[i], rBegin);
            }
          }
        }
      }
    }
    assertEquals(-1, buf.getRangeBegin(null, 0));
  }

  public void testGetRangeEnd() throws Exception {
    for (String docId : documentIds) {
      for (String annotClass : annotClasses) {
        for (String annotType : annotTypes) {
          AnnotationRecord ar = buf.getAnnotationRecord(docId, annotClass, annotType);
          for (int i = 0; i < ar.getNumEntries(); i++) {
            int rEnd = buf.getRangeEnd(ar, i);
            if (annotType.compareTo("word") == 0) {
              if (docId.compareTo("doc3") == 0 && i == 2) {
                assertEquals(21, rEnd);
              } else {
                assertEquals(rEndsWord[i], rEnd);
              }
            } else if (annotType.compareTo("space") == 0) {
              assertEquals(rEndsSpace[i], rEnd);
            }
          }
        }
      }
    }
    assertEquals(-1, buf.getRangeEnd(null, 0));
  }

  public void testGetAnnotId() throws Exception {
    for (String docId : documentIds) {
      for (String annotClass : annotClasses) {
        for (String annotType : annotTypes) {
          AnnotationRecord ar = buf.getAnnotationRecord(docId, annotClass, annotType);
          for (int i = 0; i < ar.getNumEntries(); i++) {
            int annotId = buf.getAnnotId(ar, i);
            if (annotType.compareTo("word") == 0) {
              assertEquals(aIdsWord[i], annotId);
            } else if (annotType.compareTo("space") == 0) {
              assertEquals(aIdsSpace[i], annotId);
            }
          }
        }
      }
    }

    assertEquals(-1, buf.getAnnotId(null, 0));
  }

  public void testGetMetadata() throws Exception {
    for (String docId : documentIds) {
      for (String annotClass : annotClasses) {
        for (String annotType : annotTypes) {
          AnnotationRecord ar = buf.getAnnotationRecord(docId, annotClass, annotType);
          for (int i = 0; i < ar.getNumEntries(); i++) {
            String metadata = buf.getMetadata(ar, i);
            if (annotType.compareTo("word") == 0) {
              if (docId.compareTo("doc1") == 0) {
                assertEquals(metadataDoc1Words[i], metadata);
              } else if (docId.compareTo("doc2") == 0) {
                assertEquals("", metadata);
              } else if (docId.compareTo("doc3") == 0) {
                assertEquals(metadataDoc3Words[i], metadata);
              }
            } else if (annotType.compareTo("space") == 0) {
              assertEquals("", metadata);
            }
          }
        }
      }
    }

    assertEquals(null, buf.getMetadata(null, 0));
  }

  public void testGetAnnotation() throws Exception {
    for (String docId : documentIds) {
      for (String annotClass : annotClasses) {
        for (String annotType : annotTypes) {
          AnnotationRecord ar = buf.getAnnotationRecord(docId, annotClass, annotType);
          for (int i = 0; i < ar.getNumEntries(); i++) {
            Annotation a = buf.getAnnotation(ar, i);
            int rBegin = a.getrBegin();
            int rEnd = a.getrEnd();
            int annotId = a.getId();
            String metadata = a.getMetadata();

            if (annotType.compareTo("word") == 0) {
              assertEquals(rBeginsWord[i], rBegin);
            } else if (annotType.compareTo("space") == 0) {
              assertEquals(rBeginsSpace[i], rBegin);
            }

            if (annotType.compareTo("word") == 0) {
              if (docId.compareTo("doc3") == 0 && i == 2) {
                assertEquals(21, rEnd);
              } else {
                assertEquals(rEndsWord[i], rEnd);
              }
            } else if (annotType.compareTo("space") == 0) {
              assertEquals(rEndsSpace[i], rEnd);
            }

            if (annotType.compareTo("word") == 0) {
              assertEquals(aIdsWord[i], annotId);
            } else if (annotType.compareTo("space") == 0) {
              assertEquals(aIdsSpace[i], annotId);
            }

            if (annotType.compareTo("word") == 0) {
              if (docId.compareTo("doc1") == 0) {
                assertEquals(metadataDoc1Words[i], metadata);
              } else if (docId.compareTo("doc2") == 0) {
                assertEquals("", metadata);
              } else if (docId.compareTo("doc3") == 0) {
                assertEquals(metadataDoc3Words[i], metadata);
              }
            } else if (annotType.compareTo("space") == 0) {
              assertEquals("", metadata);
            }
          }
        }
      }
    }

    assertEquals(null, buf.getAnnotation(null, 0));
  }

  public void testFindAnnotationsOver() throws Exception {
    AnnotationRecord ar1 = buf.getAnnotationRecord("doc1", "ge", "word");
    int[] res1 = buf.findAnnotationsOver(ar1, 1, 3);
    assertEquals(1, res1.length);
    assertEquals(0, res1[0]);

    AnnotationRecord ar2 = buf.getAnnotationRecord("doc3", "ge", "space");
    int[] res2 = buf.findAnnotationsOver(ar2, 8, 8);
    assertEquals(1, res2.length);
    assertEquals(0, res2[0]);

    AnnotationRecord ar3 = buf.getAnnotationRecord("doc2", "ge", "word");
    int[] res3 = buf.findAnnotationsOver(ar3, 6, 9);
    assertEquals(0, res3.length);

    int[] res4 = buf.findAnnotationsOver(null, 6, 9);
    assertEquals(0, res4.length);
  }
}
