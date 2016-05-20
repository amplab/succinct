package edu.berkeley.cs.succinct.buffers.annot;

import junit.framework.TestCase;

import java.util.Iterator;

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

  private final SuccinctAnnotationBuffer buf = new SuccinctAnnotationBuffer("ge", "word", test);
  private final String[] documentIds = {"doc1", "doc2", "doc3"};

  // (0, 8), (9, 15), (16, 19)
  private final int[] startOffsets = {0, 9, 16};
  private final int[] endOffsets = {8, 15, 19};

  private final int[] annotationIds = {1, 3, 5};
  private final int[] aOffsets = {0, 61, 113};

  private final String[] metadataDoc1 = {"foo", "bar", "baz"};
  private final String[] metadataDoc3 = {"a", "b&c", "d^e"};

  public void testGetAnnotationRecord() throws Exception {
    for (String docId : documentIds) {
      AnnotationRecord ar = buf.getAnnotationRecord(docId);
      assertNotNull(ar);
      assertEquals(docId, ar.getDocId());
      assertEquals(3, ar.getNumEntries());
    }

    assertNull(buf.getAnnotationRecord("a"));
  }

  public void testGetAnnotationRecord2() throws Exception {
    for (int i = 0; i < 3; i++) {
      AnnotationRecord ar = buf.getAnnotationRecord(aOffsets[i]);
      assertNotNull(ar);
      assertEquals(documentIds[i], ar.getDocId());
      assertEquals(3, ar.getNumEntries());
    }

    assertNull(buf.getAnnotationRecord(172));
  }

  public void testIterator() throws Exception {
    Iterator<Annotation> it = buf.iterator();
    int count = 0;
    while (it.hasNext()) {
      Annotation a = it.next();
      assertEquals(documentIds[count / 3], a.getDocId());
      assertEquals(startOffsets[count % 3], a.getStartOffset());
      assertEquals(count == 8 ? 21 : endOffsets[count % 3], a.getEndOffset());
      assertEquals(annotationIds[count % 3], a.getId());
      count++;
    }
    assertEquals(9, count);
  }

  public void testGetRangeBegin() throws Exception {
    for (String docId : documentIds) {
      AnnotationRecord ar = buf.getAnnotationRecord(docId);
      for (int i = 0; i < ar.getNumEntries(); i++) {
        int rBegin = ar.getStartOffset(i);
        assertEquals(startOffsets[i], rBegin);
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
          assertEquals(endOffsets[i], rEnd);
        }
      }
    }
  }

  public void testGetAnnotId() throws Exception {
    for (String docId : documentIds) {
      AnnotationRecord ar = buf.getAnnotationRecord(docId);
      for (int i = 0; i < ar.getNumEntries(); i++) {
        int annotId = ar.getAnnotId(i);
        assertEquals(annotationIds[i], annotId);
      }
    }
  }

  public void testGetMetadata() throws Exception {
    for (String docId : documentIds) {
      AnnotationRecord ar = buf.getAnnotationRecord(docId);
      for (int i = 0; i < ar.getNumEntries(); i++) {
        String metadata = ar.getMetadata(i);
        if (docId.compareTo("doc1") == 0) {
          assertEquals(metadataDoc1[i], metadata);
        } else if (docId.compareTo("doc2") == 0) {
          assertEquals("", metadata);
        } else if (docId.compareTo("doc3") == 0) {
          assertEquals(metadataDoc3[i], metadata);
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

        assertEquals(startOffsets[i], rBegin);

        if (docId.compareTo("doc3") == 0 && i == 2) {
          assertEquals(21, rEnd);
        } else {
          assertEquals(endOffsets[i], rEnd);
        }

        assertEquals(annotationIds[i], annotId);

        if (docId.compareTo("doc1") == 0) {
          assertEquals(metadataDoc1[i], metadata);
        } else if (docId.compareTo("doc2") == 0) {
          assertEquals("", metadata);
        } else if (docId.compareTo("doc3") == 0) {
          assertEquals(metadataDoc3[i], metadata);
        }
      }
    }
  }

  public void testFindAnnotationsContaining() throws Exception {
    AnnotationRecord ar1 = buf.getAnnotationRecord("doc1");
    Annotation[] res1 = ar1.annotationsContaining(1, 3);
    assertEquals(res1[0].getDocId(), "doc1");
    assertEquals(res1[0].getStartOffset(), 0);
    assertEquals(res1[0].getEndOffset(), 8);
    assertEquals(1, res1.length);

    AnnotationRecord ar2 = buf.getAnnotationRecord("doc2");
    Annotation[] res2 = ar2.annotationsContaining(6, 9);
    assertEquals(0, res2.length);

    AnnotationRecord ar3 = buf.getAnnotationRecord("doc3");
    Annotation[] res3 = ar3.annotationsContaining(9, 15);
    assertEquals(res3[0].getDocId(), "doc3");
    assertEquals(res3[0].getStartOffset(), 9);
    assertEquals(res3[0].getEndOffset(), 15);
    assertEquals(1, res3.length);
  }

  public void testFindAnnotationsContainedIn() throws Exception {
    AnnotationRecord ar1 = buf.getAnnotationRecord("doc1");
    int[] res1 = ar1.annotationsContainedIn(0, 15);
    assertEquals(2, res1.length);
    assertEquals(0, res1[0]);
    assertEquals(1, res1[1]);

    AnnotationRecord ar2 = buf.getAnnotationRecord("doc2");
    int[] res2 = ar2.annotationsContainedIn(6, 9);
    assertEquals(0, res2.length);

    AnnotationRecord ar3 = buf.getAnnotationRecord("doc3");
    int[] res3 = ar3.annotationsContainedIn(9, 15);
    assertEquals(1, res3.length);
    assertEquals(1, res3[0]);
  }
}
