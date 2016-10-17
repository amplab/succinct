# Succinct

[![Build Status](https://amplab.cs.berkeley.edu/jenkins/buildStatus/icon?job=Succinct)](https://amplab.cs.berkeley.edu/jenkins/job/Succinct/)

[Succinct](http://succinct.cs.berkeley.edu) is a data store that enables queries
directly on a compressed representation of data. This repository maintains the 
Java implementations of Succinct's core algorithms, and applications that 
exploit them, such as a [Apache Spark](http://spark.apache.org/) binding for Succinct.

## Building Succinct

Succinct is built using [Apache Maven](http://maven.apache.org/).
To build Succinct and its component modules, run:

    mvn clean package

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
    <version>0.1.7</version>
</dependency>
```

## Succinct on Apache Spark
We provide Apache Spark and Apache Spark SQL interfaces for Succinct, which
expose a compressed, queryable RDD `SuccinctRDD`, enabling manipulation of 
unstructured data, and a `SuccinctKVRDD` for querying semi-structured data
(key-value pairs, text and json documents, etc.). We also expose Succinct
as a DataSource in Apache Spark SQL as an experimental feature. More details on
the integration with Apache Spark can be found [here](spark/README.md).

### Dependency Information

#### Apache Maven

To build your application to run with Succinct on Apache Spark, you can link against this 
library using Apache Maven by adding the following dependency information to your pom.xml file:

```xml
<dependency>
    <groupId>amplab</groupId>
    <artifactId>succinct-spark</artifactId>
    <version>0.1.7</version>
</dependency>
```

#### SBT and Spark-Packages

Add the dependency to your SBT project by adding the following to `build.sbt` 
(see the [Spark Packages listing](http://spark-packages.org/package/amplab/succinct)
for spark-submit and Maven instructions):

```
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "amplab" % "succinct" % "0.1.7"
```

The succinct-spark jar file can also be added to a Spark shell using the 
`--jars` command line option. For example, to include it when starting the 
spark shell:

```
$ bin/spark-shell --jars succinct-0.1.7.jar
```
