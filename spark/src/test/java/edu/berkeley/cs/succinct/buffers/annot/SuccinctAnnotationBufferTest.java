package edu.berkeley.cs.succinct.buffers.annot;

import junit.framework.TestCase;

public class SuccinctAnnotationBufferTest extends TestCase {

  // See AnnotatedDocumentSerializerSuite.scala for the original annotations for (ge, word).

  private final byte[] test =
    {94, 100, 111, 99, 49, 94, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 16, 0, 0, 0, 8, 0, 0, 0,
      15, 0, 0, 0, 19, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 5, 0, 3, 102, 111, 111, 0, 3, 98, 97, 114,
      0, 3, 98, 97, 122, 94, 100, 111, 99, 50, 94, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 16,
      0, 0, 0, 8, 0, 0, 0, 15, 0, 0, 0, 19, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0,
      94, 100, 111, 99, 51, 94, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 16, 0, 0, 0, 8, 0, 0,
      0, 15, 0, 0, 0, 21, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 5, 0, 1, 97, 0, 3, 98, 38, 99, 0, 3, 100,
      94, 101};

  private final SuccinctAnnotationBuffer buf = new SuccinctAnnotationBuffer(test);
  private final String[] documentIds = {"doc1", "doc2", "doc3"};

  private final int[] rBeginsWord = {0, 9, 16};
  private final int[] rEndsWord = {8, 15, 19};
  private final int[] aIdsWord = {1, 3, 5};

  private final String[] metadataDoc1Words = {"foo", "bar", "baz"};
  private final String[] metadataDoc3Words = {"a", "b&c", "d^e"};

  public void testFindAnnotationRecord() throws Exception {
    for (String docId : documentIds) {
      AnnotationRecord ar = buf.getAnnotationRecord(docId);
      assertNotNull(ar);
      assertEquals(docId, ar.getDocId());
      assertEquals(3, ar.getNumEntries());
    }


    assertNull(buf.getAnnotationRecord("a"));
  }

  public void testGetRangeBegin() throws Exception {
    for (String docId : documentIds) {
      AnnotationRecord ar = buf.getAnnotationRecord(docId);
      for (int i = 0; i < ar.getNumEntries(); i++) {
        int rBegin = ar.getStartOffset(i);
        assertEquals(rBeginsWord[i], rBegin);
      }
    }
  }


  public void testGetRangeEnd() throws Exception {
    for (String docId : documentIds) {
      AnnotationRecord ar = buf.getAnnotationRecord(docId);
      for (int i = 0; i < ar.getNumEntries(); i++) {
        int rEnd = ar.getEndOffset(i);
        if (docId.compareTo("doc3") == 0 && i == 2) {
          assertEquals(21, rEnd);
        } else {
          assertEquals(rEndsWord[i], rEnd);
        }
      }
    }
  }

  public void testGetAnnotId() throws Exception {
    for (String docId : documentIds) {
      AnnotationRecord ar = buf.getAnnotationRecord(docId);
      for (int i = 0; i < ar.getNumEntries(); i++) {
        int annotId = ar.getAnnotId(i);
        assertEquals(aIdsWord[i], annotId);
      }
    }
  }

  public void testGetMetadata() throws Exception {
    for (String docId : documentIds) {
      AnnotationRecord ar = buf.getAnnotationRecord(docId);
      for (int i = 0; i < ar.getNumEntries(); i++) {
        String metadata = ar.getMetadata(i);
        if (docId.compareTo("doc1") == 0) {
          assertEquals(metadataDoc1Words[i], metadata);
        } else if (docId.compareTo("doc2") == 0) {
          assertEquals("", metadata);
        } else if (docId.compareTo("doc3") == 0) {
          assertEquals(metadataDoc3Words[i], metadata);
        }
      }
    }
  }

  public void testGetAnnotation() throws Exception {
    for (String docId : documentIds) {
      AnnotationRecord ar = buf.getAnnotationRecord(docId);
      for (int i = 0; i < ar.getNumEntries(); i++) {
        Annotation a = ar.getAnnotation(i);
        int rBegin = a.getStartOffset();
        int rEnd = a.getEndOffset();
        int annotId = a.getId();
        String metadata = a.getMetadata();

        assertEquals(rBeginsWord[i], rBegin);

        if (docId.compareTo("doc3") == 0 && i == 2) {
          assertEquals(21, rEnd);
        } else {
          assertEquals(rEndsWord[i], rEnd);
        }

        assertEquals(aIdsWord[i], annotId);

        if (docId.compareTo("doc1") == 0) {
          assertEquals(metadataDoc1Words[i], metadata);
        } else if (docId.compareTo("doc2") == 0) {
          assertEquals("", metadata);
        } else if (docId.compareTo("doc3") == 0) {
          assertEquals(metadataDoc3Words[i], metadata);
        }
      }
    }
  }

  public void testFindAnnotationsOver() throws Exception {
    AnnotationRecord ar1 = buf.getAnnotationRecord("doc1");
    int[] res1 = ar1.findAnnotationsContaining(1, 3);
    assertEquals(1, res1.length);
    assertEquals(0, res1[0]);

    AnnotationRecord ar2 = buf.getAnnotationRecord("doc2");
    int[] res2 = ar2.findAnnotationsContaining(6, 9);
    assertEquals(0, res2.length);
  }
}
