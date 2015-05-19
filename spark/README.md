Succinct-Spark
==============

[Spark](http://spark.apache.org/) and 
[Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html) 
interfaces for [Succinct](http://succinct.cs.berkeley.edu/). 
This library facilitates compressing RDDs in Spark and DataFrames in Spark SQL
and enables queries directly on the compressed representation.

## Requirements

This library requires Succinct 0.1.0+ and Spark 1.3+.

## Dependency Information

### Apache Maven

To build your application with Succinct-Spark, you can link against this library
using Maven by adding the following dependency information to your pom.xml file:

```xml
<dependency>
    <groupId>edu.berkeley.cs.succinct</groupId>
    <artifactId>succinct-spark</artifactId>
    <version>0.1.0</version>
</dependency>
```

### SBT

Add the dependency to your SBT project by adding the following to `build.sbt` 
(see the [Spark Packages listing](http://spark-packages.org/package/amplab/succinct-spark)
for spark-submit and Maven instructions):

```
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "edu.berkeley.cs.succinct" % "succinct-spark" % "0.1.0"
```

The succinct-spark jar file can also be added to a Spark shell using the 
`--jars` command line option. For example, to include it when starting the 
spark shell:

```
$ bin/spark-shell --jars succinct-spark_2.10-0.1.0.jar
```

## Usage

The Succinct-Spark library exposes two APIs: 
* An RDD API that provides an RDD abstraction for Succinct encoded data
* DataFrame API that integrates with the Spark SQL interface.

**Note: The Spark SQL interface is experimental, and only efficient for a few
filter queries. We aim to make the Spark SQL integration more efficient in
future releases.**

### RDD API

We expose a `SuccinctRDD` that extends `RDD[Array[Byte]]`. Since each record is
represented as an array of bytes, `SuccinctRDD` can be used to encode a 
collection of any type of records by providing a serializer/deserializer for
the record type. 

`SuccinctRDD` can be used as follows:

```scala
import edu.berkeley.cs.succinct.SuccinctRDD

// Read text data from file
val textRDD = sc.textFile("/path/to/data")

// Convert the textRDD to a SuccinctRDD after serializing each record into an
// array of bytes. Persist the RDD memory to perform in-memory queries.
val succinctTextRDD = SuccinctRDD(textRDD.map(_.getBytes)).cache

// Count the number of occurrences of "berkeley" in the data
val berkeleyCount = succinctTextRDD.count("berkeley".getBytes)

// Fetch all records that contain the string "berkeley"
val berkeleyRecords = succinctTextRDD.searchRecords("berkeley".getBytes).collect
```

### DataFrame API

The DataFrame API for Succinct is experimental for now, and only supports 
selected data types and filters. The supported SparkSQL types include:

```
BooleanType
ByteType
ShortType
IntegerType
LongType
FloatType
DoubleType
DecimalType
StringType
```

The supported SparkSQL filters include:

```
StringStartsWith
StringEndsWith
StringContains
EqualTo
LessThan
LessThanOrEqual
GreaterThan
GreaterThanOrEqual
```

Note that certain SQL operations, like joins, might be inefficient on the
DataFrame API for now. We plan on improving the performance for generic
SQL operations in a future release.

The DataFrame API can be used as follows:

```
import edu.berkeley.cs.succinct.sql

// Create a schema
val citySchema = StructType(Seq(
  StructField("Name", StringType, false),
  StructField("Length", IntegerType, true),
  StructField("Area", DoubleType, false),
  StructField("Airport", BooleanType, true)))

// Create an RDD of Rows with some data
val cityRDD = sparkContext.parallelize(Seq(
  Row("San Francisco", 12, 44.52, true),
  Row("Palo Alto", 12, 22.33, false),
  Row("Munich", 8, 3.14, true)))

// Create a data frame from the RDD and the schema
val cityDataFrame = sqlContext.createDataFrame(cityRDD, citySchema)

// Save the DataFrame in the "Succinct" format
cityDataFrame.saveAsSuccinctFiles("/path/to/data")

// Read the Succinct DataFrame from the saved path
val succinctCityRDD = sqlContext.succinctFile("/path/to/data")

// Filter and prune
val bigCities = succinctCityRDD.filter("Area >= 22.0").select("Name").collect
```
