Succinct
========

[Succinct](http://succinct.cs.berkeley.edu) is a data store that enables queries
directly on a compressed representation of data. This repository maintains the 
Java implementations of Succinct's core algorithms, and applications that 
exploit them, such as a [Spark](http://spark.apache.org/) binding for Succinct.

The master branch is in version 0.1.3.

## Building Succinct

Succinct is built using [Apache Maven](http://maven.apache.org/).
To build Succinct and its component modules, run:

    mvn clean package

Alternatively, one can also use `sbt` for building and development:

    sbt/sbt gen-idea # can now import project into Intellij IDEA
    sbt/sbt assembly # builds uber jars
    sbt/sbt "~assembly" # incremental build
    sbt/sbt "testOnly edu.berkeley.cs.succinct.sql.SuccinctSQLSuite"
    sbt/sbt "project spark" "runMain edu.berkeley.cs.succinct.examples.WikiSearch <dataPath>"

## Succinct-Core

The Succinct-Core module contains Java implementation of Succinct's core
algorithms. See a more descriptive description of the core module 
[here](core/README.md).

### Dependency Information

#### Apache Maven

To build your application with Succinct-Core, you can link against this library
using Maven by adding the following dependency information to your pom.xml file:

```xml
<dependency>
    <groupId>amplab</groupId>
    <artifactId>succinct-core</artifactId>
    <version>0.1.3</version>
</dependency>
```

## Succinct-Spark
The Succinct-Spark module contains Spark and Spark SQL intefaces for Succinct,
exposes a compressed, queryable RDD `SuccinctRDD`, which allows manipulating 
unstructured data, and a `SuccinctKVRDD` for querying semi-structured data
that can be represented as key-value pairs. We also expose Succinct
as a DataSource in Spark SQL as an experimental feature. More details on the
Succinct-Spark module can be found [here](spark/README.md).

### Dependency Information

#### Apache Maven

To build your application with Succinct-Spark, you can link against this library
using Maven by adding the following dependency information to your pom.xml file:

```xml
<dependency>
    <groupId>amplab</groupId>
    <artifactId>succinct-spark</artifactId>
    <version>0.1.3</version>
</dependency>
```

#### SBT and Spark-Packages

Add the dependency to your SBT project by adding the following to `build.sbt` 
(see the [Spark Packages listing](http://spark-packages.org/package/amplab/succinct)
for spark-submit and Maven instructions):

```
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "amplab" % "succinct" % "0.1.3"
```

The succinct-spark jar file can also be added to a Spark shell using the 
`--jars` command line option. For example, to include it when starting the 
spark shell:

```
$ bin/spark-shell --jars succinct-0.1.3.jar
```
