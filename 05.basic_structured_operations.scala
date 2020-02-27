// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession
   .builder()
   .appName("Simple Application")
   .getOrCreate()

// ---------- SPARK TYPES ---------- 
import org.apache.spark.sql.types._
val a = ByteType
val b = StringType
val c = BinaryType
val d = BooleanType
val e = DateType
// ShortType, IntegerType, LongType, FloatType, DoubleType, 
// DecimalType, TimestampType, ArrayType, MapType (= dict in Python)


// ---------- READ jSON ---------- 
val df = spark.read
  .format("json")
  .load("/FileStore/tables/2015_summary-ebaee.json")


// ---------- SCHEMA ---------- 
df.schema
org.apache.spark.sql.types.StructType = StructType(
	StructField(DEST_COUNTRY_NAME,StringType,true), 
	StructField(ORIGIN_COUNTRY_NAME,StringType,true), 
	StructField(count,LongType,true))

df.printSchema()
root
 |-- DEST_COUNTRY_NAME: string (nullable = true)
 |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
 |-- count: long (nullable = true)


// Create & enforce a specific schema
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.Metadata

val myManualSchema = StructType(Array(
    StructField("DEST_COUNTRY_NAME", StringType, true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    StructField("count", LongType, false,
    Metadata.fromJson("{\"hello\":\"world\"}"))
  ))

val df = spark.read.format("json").schema(myManualSchema)
  .load("/FileStore/tables/2015_summary-ebaee.json")

df.createOrReplaceTempView("dfTable")
df.show(3)


// ---------- COLS & EXPR ---------- 

import org.apache.spark.sql.functions._
df.select("count").show(3)
spark.sql("""SELECT count FROM dfTable LIMIT 3""").show()

// creation
col("someColumnName")
org.apache.spark.sql.Column = someColumnName

println(df.col("count"))
count

df.columns
res9: Array[String] = Array(DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count)

// Expression
df.select(expr("count + 5")).show(3)
spark.sql("""SELECT count + 5 FROM dfTable LIMIT 3""").show()
+-----------+
|(count + 5)|
+-----------+
|         20|
|          6|
|        349|
+-----------+

// Records and Rows
import org.apache.spark.sql.Row

val myRow = Row("Hello", null, 1, false)

myRow(0)
res11: Any = Hello

myRow.getString(0)
res12: String = Hello

myRow.getString(1)
res13: String = null

myRow.getInt(2)
res14: Int = 1


// ---------- DF TRANSFORMATIONS ---------- 

val myManualSchema = new StructType(Array(
  new StructField("some", StringType, true),
  new StructField("col", StringType, true),
  new StructField("names", LongType, false)))

val myRows = Seq(Row("Hello", null, 1L))

val myRDD = spark.sparkContext.parallelize(myRows)

val myDf = spark.createDataFrame(myRDD, myManualSchema)

myDf.show()

+-----+----+-----+
| some| col|names|
+-----+----+-----+
|Hello|null|    1|
+-----+----+-----+


val myDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")

df.select("count").show(2)
+-----+
|count|
+-----+
|   15|
|    1|
+-----+


df.select("DEST_COUNTRY_NAME", "count").show(3)
spark.sql("SELECT DEST_COUNTRY_NAME, count FROM dfTable LIMIT 3").show()
+-----------------+-----+
|DEST_COUNTRY_NAME|count|
+-----------------+-----+
|    United States|   15|
|    United States|    1|
|    United States|  344|
+-----------------+-----+


df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
df.select(expr("DEST_COUNTRY_NAME").alias("destination")).show(2)
spark.sql("SELECT DEST_COUNTRY_NAME AS destination FROM dfTable LIMIT 2").show()
+-------------+
|  destination|
+-------------+
|United States|
|United States|
+-------------+


df.selectExpr("*", "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as whitinCountry").show(2)
spark.sql("""
  SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry
  FROM dfTable
  LIMIT 2
""").show(2)
+-----------------+-------------------+-----+-------------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|whitinCountry|
+-----------------+-------------------+-----+-------------+
|    United States|            Romania|   15|        false|
|    United States|            Croatia|    1|        false|
+-----------------+-------------------+-----+-------------+


df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show()
spark.sql("SELECT avg(count), count(distinct(DEST_COUNTRY_NAME)) FROM dfTable").show()
+-----------+---------------------------------+
| avg(count)|count(DISTINCT DEST_COUNTRY_NAME)|
+-----------+---------------------------------+
|1770.765625|                              132|
+-----------+---------------------------------+


// Converting to Spark Types (Literals)
import org.apache.spark.sql.functions.lit
df.select(expr("*"), lit(1).as("One")).show(2)
spark.sql("SELECT *, 1 as One FROM dfTable LIMIT 2").show()
+-----------------+-------------------+-----+---+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|One|
+-----------------+-------------------+-----+---+
|    United States|            Romania|   15|  1|
|    United States|            Croatia|    1|  1|
+-----------------+-------------------+-----+---+


// Adding Columns
df.withColumn("numberOne", lit("ones")).show(2)
spark.sql("SELECT *, CAST('ones' as varchar(4)) as numberOne FROM dfTable LIMIT 2").show()
+-----------------+-------------------+-----+---------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|numberOne|
+-----------------+-------------------+-----+---------+
|    United States|            Romania|   15|     ones|
|    United States|            Croatia|    1|     ones|
+-----------------+-------------------+-----+---------+


df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)
+-----------------+-------------------+-----+-------------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|withinCountry|
+-----------------+-------------------+-----+-------------+
|    United States|            Romania|   15|        false|
|    United States|            Croatia|    1|        false|
+-----------------+-------------------+-----+-------------+


// Renaming Columns
df.withColumn("This Long Column-Name", expr("ORIGIN_COUNTRY_NAME")).show(2)
+-----------------+-------------------+-----+---------------------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|This Long Column-Name|
+-----------------+-------------------+-----+---------------------+
|    United States|            Romania|   15|              Romania|
|    United States|            Croatia|    1|              Croatia|
+-----------------+-------------------+-----+---------------------+


// By default Spark is case insensitive
set spark.sql.caseSensitive true

// Removing Columns
df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").columns

// Changing a Column’s Type (cast)
df.withColumn("count2", col("count").cast("long")).show(2)
spark.sql("SELECT *, cast(count as long) AS count2 FROM dfTable").show(2)
+-----------------+-------------------+-----+------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|count2|
+-----------------+-------------------+-----+------+
|    United States|            Romania|   15|    15|
|    United States|            Croatia|    1|     1|
+-----------------+-------------------+-----+------+


// Filtering Rows
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)
spark.sql("SELECT * FROM dfTable WHERE count < 2 LIMIT 2").show()
+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Croatia|    1|
|    United States|          Singapore|    1|
+-----------------+-------------------+-----+


df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(2)
spark.sql("""
  SELECT * FROM dfTable WHERE count < 2 AND ORIGIN_COUNTRY_NAME != "Croatia"
  LIMIT 2""").show()
+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|          Singapore|    1|
|          Moldova|      United States|    1|
+-----------------+-------------------+-----+


// Getting Unique Rows
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
res50: Long = 256

spark.sql("SELECT COUNT(DISTINCT(ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME)) FROM dfTable").show()
+----------------------------------------------------------------------+
|count(DISTINCT named_struct(ORI...AME, ORI...AME, DES...ME, DES...ME))|
+----------------------------------------------------------------------+
|                                                                   256|
+----------------------------------------------------------------------+

df.select("ORIGIN_COUNTRY_NAME").distinct().count()
spark.sql("SELECT COUNT(DISTINCT ORIGIN_COUNTRY_NAME) FROM dfTable").show()
+-----------------------------------+
|count(DISTINCT ORIGIN_COUNTRY_NAME)|
+-----------------------------------+
|                                125|
+-----------------------------------+

// Random Samples
val seed = 5
val withReplacement = false
val fraction = 0.5
df.sample(withReplacement, fraction, seed).count()


// Random Splits
val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
dataFrames(0).count() > dataFrames(1).count() // False


import org.apache.spark.sql.Row
val schema = df.schema
val newRows = Seq(
  Row("New Country", "Other Country", 5L),
  Row("New Country 2", "Other Country 3", 1L)
)
val parallelizedRows = spark.sparkContext.parallelize(newRows)
val newDF = spark.createDataFrame(parallelizedRows, schema)

df.union(newDF)
  .where("count = 1")
  .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
  .show() // get all of them and we'll see our new rows at the end
+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Croatia|    1|
     ...                       ...         .
|    United States|            Namibia|    1|
|    New Country 2|    Other Country 3|    1|
+-----------------+-------------------+-----+


// Sorting Rows
df.sort(desc("count")).show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
+-----------------+-------------------+------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME| count|
+-----------------+-------------------+------+
|    United States|      United States|370002|
|    United States|             Canada|  8483|


import org.apache.spark.sql.functions.{desc, asc}
df.orderBy(expr("count desc")).show(2)
df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)
spark.sql("""SELECT * FROM dfTable ORDER BY count DESC, DEST_COUNTRY_NAME ASC LIMIT 2""").show()
+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|          Moldova|      United States|    1|
|    United States|            Croatia|    1|
+-----------------+-------------------+-----+

+-----------------+-------------------+------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME| count|
+-----------------+-------------------+------+
|    United States|      United States|370002|
|    United States|             Canada|  8483|
+-----------------+-------------------+------+


// Limit
df.limit(3).show()
spark.sql("SELECT * FROM dfTable LIMIT 3").show()
+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Romania|   15|
|    United States|            Croatia|    1|
|    United States|            Ireland|  344|
+-----------------+-------------------+-----+

	
// Repartition and Coalesce
df.rdd.getNumPartitions // 1

val tempDf = df.repartition(5)
tempDf.rdd.getNumPartitions

df.repartition(5, col("DEST_COUNTRY_NAME"))

// Coalesce, on the other hand, will not incur a full shuffle
// and will try to combine partitions. This
df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

// Collecting some Rows to the Driver
// in order to manipulate it on your local machine.
val collectDF = df.limit(10)
collectDF.take(5)
collectDF.show()

// iterate over the entire dataset partition-by-partition in a serial manner:
collectDF.toLocalIterator()