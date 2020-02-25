// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Spark basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

// COMMAND ----------

val df = spark.read.format("json").load("/FileStore/tables/2015_summary-ebaee.json")
newDF.show(5)

// COMMAND ----------

df.schema

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// DBTITLE 1,Columns and Expressions
import org.apache.spark.sql.functions._

// Columns
df.col("DEST_COUNTRY_NAME")
col("DEST_COUNTRY_NAME")

// COMMAND ----------

df.col("DEST_COUNTRY_NAME").getClass()

// COMMAND ----------

// Explicit column references
df.col("count")

// COMMAND ----------

df.columns

// COMMAND ----------

df.columns(0)

// COMMAND ----------

// Expressions
expr("count" + 5)

// COMMAND ----------

df.expr("count" + 5).show(5)

// COMMAND ----------

// DBTITLE 1,Records and Rows
df.first()

// COMMAND ----------

// Creating Rows
import org.apache.spark.sql.Row
val myRow = Row("Hello", null, 1, false)

// COMMAND ----------

// DBTITLE 1,DataFrame Transformations
df.createOrReplaceTempView("dfTable")

// COMMAND ----------

df.select("DEST_COUNTRY_NAME").show(2)
spark.sql("SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2").show()

// COMMAND ----------

df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)
spark.sql("SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME FROM dfTable LIMIT 2").show()

// COMMAND ----------



// COMMAND ----------

df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
spark.sql("SELECT DEST_COUNTRY_NAME as destination FROM dfTable LIMIT 2").show()

// COMMAND ----------

df.select(expr("DEST_COUNTRY_NAME").alias("destination_country_renamed")).show(2)

// COMMAND ----------

df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

// COMMAND ----------

df.selectExpr(
  "*", // include all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
  .show(2)

spark.sql("""
SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry
FROM dfTable
LIMIT 2
""").show(2)

// COMMAND ----------

df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
spark.sql("SELECT avg(count), count(distinct(DEST_COUNTRY_NAME)) FROM dfTable LIMIT 2").show()

// COMMAND ----------



// COMMAND ----------

// Converting to Spark Types (Literals)
// Sometimes, we need to pass explicit values into Spark that are just a value (rather than a new column). 
// This might be a constant value or something we’ll need to compare to later on. 
df.select(expr("*"), lit(11).as("ELEVEN")).show(2)
spark.sql("SELECT *, 11 as Eleven FROM dfTable LIMIT 2").show()

// COMMAND ----------

// Adding Columns
df.withColumn("numberOne", lit(1)).show(2)
spark.sql("SELECT *, 1 as numberOne FROM dfTable LIMIT 2").show()

// COMMAND ----------

// withColumn function takes two arguments: the column name and the expression that will create the value
df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
  .show(2)

// COMMAND ----------

// Renaming Columns
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

// COMMAND ----------

val dfWithLongColName = df.withColumn(
  "This Long Column-Name",
  expr("ORIGIN_COUNTRY_NAME"))

dfWithLongColName.show(2)

// COMMAND ----------

// Reserved Characters and Keywords
dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")
  .show(2)

// COMMAND ----------

// Case Sensitivity - By default Spark is case insensitive; however, you can change it
set spark.sql.caseSensitive true

// COMMAND ----------

// Removing Columns
df.drop("ORIGIN_COUNTRY_NAME").columns
dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").columns

// COMMAND ----------

// Changing a Column’s Type (cast)
df.withColumn("count2", col("count").cast("long")).show(2)
spark.sql("SELECT *, cast(count as long) AS count2 FROM dfTable").show(2)

// COMMAND ----------

// Filtering Rows
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)
spark.sql("SELECT * FROM dfTable WHERE count < 2 LIMIT 2").show()

// COMMAND ----------

df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(2)
spark.sql("SELECT * FROM dfTable WHERE count < 2 AND ORIGIN_COUNTRY_NAME != 'Croatia' LIMIT 2").show()

// COMMAND ----------

// Getting Unique Rows
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count() // long
spark.sql("SELECT COUNT(DISTINCT(ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME)) FROM dfTable").show() // table

// COMMAND ----------

// Random Samples
val seed = 5
val withReplacement = false
val fraction = 0.5
df.sample(withReplacement, fraction, seed).count()

// COMMAND ----------

val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
df.count()             // 256
dataFrames(0).count()  // 60
dataFrames(1).count()  // 196

// COMMAND ----------

// Concatenating and Appending Rows (Union)

// DF are immutable: users cannot append to DF because that would be changing it. 
// You must union the original DataFrame along with the new DataFrame 
// Be sure that they have the same schema and number of columns.
// Unions are performed based on location, not on the schema: 
// columns will not automatically line up the way you think they might.


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
  .show(20) // get all of them and we'll see our new rows at the end

// Scala =!= operator you don’t just compare the unevaluated column expression to a string but instead to the evaluated one

// COMMAND ----------

// Sorting Rows
df.sort("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)

// COMMAND ----------

df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)

// COMMAND ----------

// Limit - restrict what you extract from a DF
df.limit(5).show()
spark.sql("SELECT * FROM dfTable LIMIT 6").show()

// COMMAND ----------

// MIssing Repartition and Coalesce and other things ......

// COMMAND ----------


