Succinct-Spark
==============

[Spark](http://spark.apache.org/) and 
[Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html) 
interfaces for [Succinct](http://succinct.cs.berkeley.edu/). 
This library facilitates compressing RDDs in Spark and DataFrames in Spark SQL
and enables queries directly on the compressed representation.

## Requirements

This library requires Spark 1.4+.

## Dependency Information

### Apache Maven

To build your application with Succinct-Spark, you can link against this library
using Maven by adding the following dependency information to your pom.xml file:

```xml
<dependency>
    <groupId>amplab</groupId>
    <artifactId>succinct-spark</artifactId>
    <version>0.1.2</version>
</dependency>
```

### SBT

Add the dependency to your SBT project by adding the following to `build.sbt` 
(see the [Spark Packages listing](http://spark-packages.org/package/amplab/succinct)
for spark-submit and Maven instructions):

```
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "amplab" % "succinct" % "0.1.2"
```

The succinct-spark jar file can also be added to a Spark shell using the 
`--jars` command line option. For example, to include it when starting the 
spark shell:

```
$ bin/spark-shell --jars succinct-0.1.2.jar
```

## Usage

The Succinct-Spark library exposes three APIs: 
* A SuccinctRDD API that views an RDD as an unstructured "flat-file" and enables queries on its compressed representation.
* A SuccinctKVRDD API that provides a key-value abstraction for the data, and supports search and random-access over the _values_.
* DataFrame API that integrates with the Spark SQL interface via Data Sources, and supports SQL queries on compressed structured data.

**Note: The Spark SQL interface is experimental, and only efficient for selected
SQL operators. We aim to make the Spark SQL integration more efficient in
future releases.**

### SuccinctRDD API

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
// array of bytes. Persist the RDD in memory to perform in-memory queries.
val succinctTextRDD = SuccinctRDD(textRDD.map(_.getBytes)).cache

// Count the number of records containing "Succinct" in the data
val succinctCount = succinctTextRDD.count("Succinct")

// Fetch all records that contain the string "Succinct"
val succinctRecords = succinctTextRDD.search("Succinct").collect
```

More examples of the SuccinctRDD API can be found [here](src/main/scala/edu/berkeley/cs/succinct/examples/WikiSearch.scala).

#### Input Constraints

We don't support non-ASCII characters in the input for now, since the
algorithms depend on using certain non-ASCII characters as internal symbols.

#### Construction Time

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

### SuccinctKVRDD API

The `SuccinctKVRDD` implements the `RDD[(K, Array[Byte]]` interface, where key
can be of the specified (_ordered_) type while the value is a serialized array of 
bytes.  

`SuccinctKVRDD` can be used as follows:

```scala
import edu.berkeley.cs.succinct.kv.SuccinctKVRDD

// Create a set of key value pairs; sc is the SparkContext
val kvRDD = sc.textFile("README.md").zipWithIndex.map(t => (t._2, t._1.getBytes))

// Convert the kvRDD to a SuccinctKVRDD.
val succinctKVRDD = SuccinctKVRDD(kvRDD).cache

// Fetch keys corresponding to values containing the string "Succinct"
val keys = succinctKVRDD.search("Succinct")

// Fetch the value correspodning to key 0
val value = succinctKVRDD.get(0)
```

More examples of the SuccinctKVRDD API can be found [here](src/main/scala/edu/berkeley/cs/succinct/examples/KVSearch.scala). 

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
import edu.berkeley.cs.succinct.sql._

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
val succinctCities = sqlContext.succinctFile("/path/to/data")

// Filter and prune
val bigCities = succinctCities.filter("Area >= 22.0").select("Name").collect

// Alternately, use the DataFrameReader API:
cityDataFrame.write.format("edu.berkeley.cs.succinct.sql").save("/path/to/data")
val succinctCities2 = sqlContext.read.format("edu.berkeley.cs.succinct.sql").load("/path/to/data")
val smallCities = succinctCities2.filter("Area <= 10.0").select("Name").collect
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

The [KV Search](src/main/scala/edu/berkeley/cs/succinct/examples/KVSearch.scala)
and [Table Search](src/main/scala/edu/berkeley/cs/succinct/examples/TableSearch.scala)
examples are executed similarly.

```
./bin/table-search [num-partitions]
```
