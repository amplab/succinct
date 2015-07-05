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

// Read text data from file; sc is the SparkContext
val textRDD = sc.textFile("README.md")

// Convert the textRDD to a SuccinctRDD after serializing each record into an
// array of bytes. Persist the RDD memory to perform in-memory queries.
val succinctTextRDD = SuccinctRDD(textRDD.map(_.getBytes)).cache

// Count the number of occurrences of "Succinct" in the data
val succinctCount = succinctTextRDD.count("Succinct".getBytes)

// Fetch all records that contain the string "Succinct"
val succinctRecords = succinctTextRDD.searchRecords("Succinct".getBytes).collect
```

#### Input Constraints

We don't support non-ASCII characters in the input for now, since the
algorithms depend on using certain non-ASCII characters as internal symbols.

### Construction Time

Another constraint to consider is the construction time for Succinct
data-structures. As for any block compression scheme, Succinct requires
non-trivial amount of time to compress an input dataset. It is strongly
advised that the SuccinctRDD be cached in memory (using RDD.cache()) 
and persisted on disk after construcion completes, to be able to re-use 
the constructed data-structures without trigerring re-construction:

```scala
import edu.berkeley.cs.succinct.SuccinctRDD
import org.apache.spark.storage.StorageLevel

// Construct the succinct RDD as before, and save it as follows
succinctTextRDD.save("/README.md.succinct")

// Load into memory again as follows; sc is the SparkContext
val loadedSuccinctRDD = SuccinctRDD(sc, "/README.md.succinct", StorageLevel.MEMORY_ONLY)
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

```scala
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

## Example Programs

The Succinct-Spark packages includes a few 
[examples](src/main/scala/edu/berkeley/cs/succinct/examples/) that elucidate the
usage of its API. To run these examples, we provide convenient scripts to run
them in the `bin/` directory. In particular, to execute the 
[Wikipedia Search](src/main/scala/edu/berkeley/cs/succinct/examples/WikiSearch.scala) 
example using SuccinctRDD, run as follows:

```
./bin/wiki-search [num-partitions]
```

The `num-partitions` parameter is simply the number of partitions that the
original dataset should be divided into for creating Succinct data structures.
This defaults to 1 by default; **note that due to Java constraints, we do not
support partitions of sizes greater than 2GB yet.**

Similarly, to execute the 
[Table Search](src/main/scala/edu/berkeley/cs/succinct/examples/TableSearch.scala)
example, run as follows:

```
./bin/table-search [num-partitions]
```
