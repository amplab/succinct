package edu.berkeley.cs.succinct.annot

import edu.berkeley.cs.succinct.buffers.annot.Annotation

abstract class Operator

case class Search(query: String) extends Operator

case class Regex(query: String) extends Operator

case class FilterAnnotations(annotClassFilter: String, annotTypeFilter: String, metadataFilter: String => Boolean) extends Operator

case class Contains(A: Operator, B: Operator) extends Operator

case class ContainedIn(A: Operator, B: Operator) extends Operator

case class Before(A: Operator, B: Operator, distance: Int = -1) extends Operator

case class After(A: Operator, B: Operator, distance: Int = -1) extends Operator

case class Result(docId: String, startOffset: Int, endOffset: Int, annotation: Annotation)
