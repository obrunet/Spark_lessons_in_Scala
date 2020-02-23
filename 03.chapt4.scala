// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Spark example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

// COMMAND ----------

// DBTITLE 1,Spark Types
val newDF = spark.range(20).toDF("number")
newDF.select(newDF.col("number") + 10).show(5)

// COMMAND ----------

newDF.collect()

// COMMAND ----------

import org.apache.spark.sql.types._

val b = ByteType

// COMMAND ----------

print(b.getClass)

// COMMAND ----------

val newDF = spark.read.json("/FileStore/tables/2015_summary-ebaee.json")
newDF.show(5)

// COMMAND ----------

// DBTITLE 1,Get schema
newDF.printSchema()

// COMMAND ----------

val df = spark.read.format("json")
  .load("/FileStore/tables/2015_summary-ebaee.json")

df.show(2)

// COMMAND ----------

spark.read.format("json").load("/FileStore/tables/2015_summary-ebaee.json").schema

// COMMAND ----------

// DBTITLE 1,Define your own schema
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata

val myManualSchema = StructType(Array(
  StructField("DEST_COUNTRY_NAME", StringType, true),
  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
  StructField("count", LongType, false,
    Metadata.fromJson("{\"hello\":\"world\"}"))
))

val df = spark.read.format("json").schema(myManualSchema)
  .load("/FileStore/tables/2015_summary-ebaee.json")

df.show(5)

// COMMAND ----------

import org.apache.spark.sql.functions.{col, column}

df.select(df.col("DEST_COUNTRY_NAME")).show(5)

// COMMAND ----------

df.select(df.col("count") + 5).show(5)

// COMMAND ----------

df.columns

// COMMAND ----------

// DBTITLE 1,Rows
df.first()

// COMMAND ----------

import org.apache.spark.sql.Row

// you must specify the values in the same order as the schema of the DataFrame to which they might be appended
val myRow = Row("Hello", null, 1, false)
print(myRow)

// COMMAND ----------

print(myRow(0))
print(myRow(1))
print(myRow(2))

// COMMAND ----------

myRow.getString(1)

// COMMAND ----------

myRow.getInt(2)

// COMMAND ----------

df.createOrReplaceTempView("dfTable")

// COMMAND ----------

val newDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")
newDF.show()

// COMMAND ----------

df.show(5)

// COMMAND ----------

df.select("DEST_COUNTRY_NAME").show(5)

// COMMAND ----------

df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(5)

// COMMAND ----------

import org.apache.spark.sql.functions._

// expr is the most flexible reference that we can use. It can refer to a plain column or a string manipulation of a column
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

// COMMAND ----------

df.select(expr("DEST_COUNTRY_NAME as destination").alias("destination_name"))
  .show(2)

// COMMAND ----------


