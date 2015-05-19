Succinct-Core
=============

Java implementation of Succinct's core algorithms. This library provides the
core algorithms for Succinct as described in the [NSDI'15 paper](https://www.usenix.org/conference/nsdi15/technical-sessions/presentation/agarwal).

## Requirements
This library has no external requirements.

## Dependency Information

### Apache Maven

To build your application with Succinct-Core, you can link against this library
using Maven by adding the following dependency information to your pom.xml file:

```xml
<dependency>
    <groupId>edu.berkeley.cs.succinct</groupId>
    <artifactId>succinct-core</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Usage

The Succinct-Core library exposes the Succinct algorithms at three layers:

```
SuccinctCore
SuccinctBuffer
SuccinctIndexedBuffer
```

### SuccinctCore

`SuccinctCore` exposes the basic construction primitive for all internal 
internal data-structures, along with accessors to the core data-structures 
(e.g., NPA, SA and ISA, which are termed as NextCharIdx, Input2AOS and AOS2Input
in the [paper](https://www.usenix.org/conference/nsdi15/technical-sessions/presentation/agarwal)).

### SuccinctBuffer

`SuccinctBuffer` builds on top of `SuccinctCore` and implements algorithms for
three basic functionalities:

```
byte[] extract(int offset, int length)
long[] search(byte[] query)
long count(byte[] query)
```

These primitives allow random access (`extract`) and search (`count`, `search`)
directly on the compressed representation of flat-file (i.e., unstructured) 
data. Look at this [example](src/main/java/edu/berkeley/cs/succinct/examples/SuccinctShell.java)
to see how `SuccinctBuffer` can be used.

### SuccinctIndexedBuffer

Finally, `SuccinctIndexedBuffer` uses the functionality of both `SuccinctCore`
and `SuccinctBuffer` to implement a record buffer, i.e., a collection of records.
This interface is used heavily in the [Succinct-Spark](../spark) library,
particularly in the [SuccinctRDD](../spark/src/main/scala/edu/berkeley/cs/succinct/SuccinctRDD.scala) 
and [SuccinctTableRDD](../spark/src/main/scala/edu/berkeley/cs/succinct/sql/SuccinctTableRDD.scala) 
implementations.
