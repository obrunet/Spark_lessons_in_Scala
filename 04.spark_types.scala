// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession
   .builder()
   .appName("Simple Application")
   .getOrCreate()

// COMMAND ----------

// Spark Types

import org.apache.spark.sql.types._

val a = ByteType
val b = StringType
val c = BinaryType
val d = BooleanType
val e = DateType
// ShortType, IntegerType, LongType, FloatType, DoubleType, 
// DecimalType, TimestampType, ArrayType, MapType (= dict in Python)

// COMMAND ----------


