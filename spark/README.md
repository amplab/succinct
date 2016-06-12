# Succinct on Apache Spark

[Apache Spark](http://spark.apache.org/) and 
[Apache Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html) 
interfaces for [Succinct](http://succinct.cs.berkeley.edu/). 
This module facilitates compressing RDDs in Apache Spark and DataFrames in Apache Spark SQL
and enables queries directly on their compressed representations.

## Requirements

This library requires Apache Spark 1.6+.

## Dependency Information

### Apache Maven

To build your application to run with Succinct on Apache Spark, you can link against this 
library using Apache Maven by adding the following dependency information to your pom.xml file:

```xml
<dependency>
    <groupId>amplab</groupId>
    <artifactId>succinct-spark</artifactId>
    <version>0.1.6</version>
</dependency>
```

### SBT

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

## Usage

The Succinct on Apache Spark exposes the following APIs: 
* A SuccinctRDD API that views an RDD as an unstructured "flat-file" and enables queries on its compressed representation.
* A SuccinctKVRDD API that provides a key-value abstraction for the data, and supports search and random-access over the _values_.
* A SuccinctJsonRDD API that enables random access and search on a collection of compressed JSON documents.
* DataFrame API that integrates with Apache Spark SQL interface via Data Sources, and supports SQL queries on compressed structured data.

**Note: The Apache Spark SQL interface is experimental, and only efficient for selected
SQL operators. We aim to make the Apache Spark SQL integration more efficient in
future releases.**

### SuccinctRDD API

We expose a `SuccinctRDD` that extends `RDD[Array[Byte]]`. Since each record is
represented as an array of bytes, `SuccinctRDD` can be used to encode a 
collection of any type of records by providing a serializer/deserializer for
the record type. 

`SuccinctRDD` can be used as follows:

```scala
import edu.berkeley.cs.succinct._

// Read text data from file; sc is the SparkContext
val wikiData = sc.textFile("/path/to/data").map(_.getBytes)

// Converts the wikiData RDD to a SuccinctRDD, serializing each record into an
// array of bytes. We persist the RDD in memory to perform in-memory queries.
val wikiSuccinctData = wikiData.succinct.persist()

// Count the number of occurrences of "Berkeley" in the RDD
val berkeleyOccCount = wikiSuccinctData.count("Berkeley")
println("# of times Berkeley appears in text = " + berkeleyOccCount)

// Find all offsets of occurrences of "Berkeley" in the RDD
val searchOffsets = wikiSuccinctData.search("Berkeley")
println("First 10 locations in the RDD where Berkeley occurs: ")
searchOffsets.take(10).foreach(println)

// Find all occurrences of the regular expression "(berkeley|stanford)\\.edu"
val regexOccurrences = wikiSuccinctData.regexSearch("(stanford|berkeley)\\.edu").collect()
println("# of matches for the regular expression (stanford|berkeley)\\.edu = " + regexOccurrences.count)

// Extract 10 bytes at offset 5 in the RDD
val extractedData = wikiSuccinctData.extract(5, 10)
println("Extracted data = [" + new String(extractedData) + "]")
```

#### Input Constraints

We don't support non-ASCII characters in the input for now, since the
algorithms depend on using certain non-ASCII characters as internal symbols.

#### Construction Time

Another constraint to consider is the construction time for Succinct
data-structures. Similar to any block compression scheme, Succinct requires
non-trivial amount of time to compress an input dataset. It is strongly
advised that the SuccinctRDD be cached in memory (using RDD.cache()) 
and persisted on disk after construcion completes, to be able to re-use 
the constructed data-structures without trigerring re-construction:

```scala
import edu.berkeley.cs.succinct._

// Read text data from file; sc is the SparkContext
val wikiData = sc.textFile("/path/to/data").map(_.getBytes)

// Construct the succinct RDD and save it as follows
wikiData.saveAsSuccinctFile("/path/to/data")

// Load into memory again as follows; sc is the SparkContext
val loadedSuccinctRDD = sc.succinctFile("/path/to/data")
```

### SuccinctKVRDD API

The `SuccinctKVRDD` implements the `RDD[(K, Array[Byte]]` interface, where key
can be of the specified (_ordered_) type while the value is a serialized array of 
bytes.

`SuccinctKVRDD` can be used as follows:

```scala
import edu.berkeley.cs.succinct.kv._

// Load data from file; sc is the SparkContext
val wikiData = sc.textFile("/path/to/data").map(_.getBytes)
val wikiKVData = wikiData.zipWithIndex().map(t => (t.\_2, t.\_1))

// Convert to SuccinctKVRDD
val succinctKVRDD = wikiKVData.succinctKV

// Get the value for key 0
val value = succinctKVRDD.get(0)
println("Value corresponding to key 0 = " + new String(value))

// Fetch 3 bytes at offset 1 for the value corresponding to key = 0
val valueData = succinctKVRDD.extract(0, 1, 3)
println("Value data for key 0 at offset 1 and length 3 = " + new String(valueData))

// count the number of occurrences of "Berkeley" accross all values
val count = succinctKVRDD.count("Berkeley")
println("Number of times Berkeley occurs in the values: " + count)

// Get the individual occurrences of Berkeley as offsets into each value
val searchOffsets = succinctKVRDD.searchOffsets("Berkeley")
println("First 10 matches for Berkeley as (key, offset) pairs: ")
searchOffsets.take(10).foreach(println)

// Search for values containing "Berkley", and fetch corresponding keys
val keys = succinctKVRDD.search("Berkeley")
println("First 10 keys matching the search query:")
keys.take(10).foreach(println)

// Regex search to find values containing matches of "(stanford|berkeley)\\.edu", 
// and fetch the corresponding of keys
val regexKeys = succinctKVRDD.regexSearch("(stanford|berkeley)\\.edu")
println("First 10 keys matching the regex query:")
regexKeys.take(10).foreach(println)
``` 

Similar to the flat-file interface, we suggest that the KV data be persisted to 
disk for repeated-use scenarios:

```scala
import edu.berkeley.cs.succinct.kv._

// Read data from file; sc is the SparkContext
val wikiData = sc.textFile("/path/to/data").map(_.getBytes)
val wikiKVData = wikiData.zipWithIndex().map(t => (t.\_2, t.\_1))

// Construct the SuccinctKVRDD and save it as follows
wikiKVData.saveAsSuccinctKV("/path/to/data")

// Load into memory again as follows; sc is the SparkContext
val loadedSuccinctKVRDD = sc.succinctKV("/path/to/data")
```

### SuccinctJsonRDD API

`SuccinctJsonRDD` provides support for JSON documents, and enables queries over a compressed, disributed JSON dataset.

When an RDD of JSON strings is converted to a `SuccinctJsonRDD`, each document is assigned a unique `id` field. All documents are indexed by this `id` field, and enable the following operations:

* The `get` operation returns a JSON document given its `id`.
* `search` and `filter` operations yeild an RDD of ids corresponding to a search term or a field value, respectively.

`SuccinctJsonRDD` can be used as follows:

```scala
import edu.berkeley.cs.succinct.json._

// Read JSON data from file; sc is the SparkContext
val jsonData = sc.textFile("/path/to/data")

// Convert to SuccinctJsonRDD
val succinctJsonRDD = jsonData.succinctJson

// Get a particular JSON document
val value = succinctJsonRDD.get(0)
println("Value corresponding to Ids 0 = " + new String(value))

// Search across JSON Documents
val ids1 = succinctJsonRDD.search("Cookie")
println("Ids matching the search query:")
ids1.foreach(println)

// Filter on attributes
val ids2 = succinctJsonRDD.filter("location.city", "Berkeley")
println("Ids matching the filter query:")
ids2.foreach(println)
``` 

For repeated-use scenarios, persist the `SuccinctJsonRDD` to disk as follows:

```scala
import edu.berkeley.cs.succinct.kv._

// Read data from file; sc is the SparkContext
val jsonData = sc.textFile("/path/to/data")

// Construct the SuccinctKVRDD and save it as follows
jsonData.saveAsSuccinctJson("/path/to/data")

// Load into memory again as follows; sc is the SparkContext
val loadedSuccinctJsonRDD = sc.succinctJson("/path/to/data")
```

### DataFrame API

The DataFrame API for Succinct is experimental for now, and only supports 
selected data types and filters. The supported Apache Spark SQL data types include:

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

The supported filters include:

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

// Create an RDD of Rows with some data; sc is the SparkContext
val cityRDD = sc.parallelize(Seq(
  Row("San Francisco", 12, 44.52, true),
  Row("Palo Alto", 12, 22.33, false),
  Row("Munich", 8, 3.14, true)))

// Create a data frame from the RDD and the schema
val cityDataFrame = sqlContext.createDataFrame(cityRDD, citySchema)

// Save the DataFrame in the "Succinct" format
cityDataFrame.write.format("edu.berkeley.cs.succinct.sql").save("/path/to/data")

// Read the Succinct DataFrame from the saved path
val succinctCities = sqlContext.succinctTable("/path/to/data")

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
