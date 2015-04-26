Succinct
========

Succinct is a Distributed-Data Store that enables queries directly on a
compressed representation of data. This repository maintains the Java
implementations of Succinct's core algorithms, and applications that exploit
them, such as a [Spark](http://spark.apache.org/) binding for Succinct.

The master branch is in version 0.1.0-SNAPSHOT.

<http://succinct.cs.berkeley.edu>

## Building Succinct

Succinct is built using [Apache Maven](http://maven.apache.org/).
To build Succinct and its example programs, run:

    mvn clean package

Alternatively, one can also use `sbt` for building and development:

    sbt/sbt gen-idea # can now import project into Intellij IDEA
    sbt/sbt assembly # builds uber jars
    sbt/sbt "~assembly" # incremental build
    sbt/sbt "testOnly edu.berkeley.cs.succinct.sql.SuccinctSQLSuite"
    sbt/sbt "project spark" "runMain edu.berkeley.cs.succinct.examples.WikiSearch <dataPath>"
