package edu.berkeley.cs.succinct.regex.parser;

public enum RegExType {
  Union,
  Concat,
  Repeat,
  Wildcard,
  CharRange,
  Primitive,
  Blank
}
