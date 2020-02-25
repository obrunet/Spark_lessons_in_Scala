// Databricks notebook source
// DBTITLE 1,Create a spark session
import org.apache.spark.sql.SparkSession

val spk = SparkSession.builder
  .master("local")
  .appName("example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

// COMMAND ----------

// DBTITLE 1,Create a DF from range
val newDF = spk.range(1000).toDF("count")
newDF.show(5)

// COMMAND ----------

// DBTITLE 1,Filter only values divisible by 2
val divisBy2 = newDF.where("count % 2 = 0")
divisBy2.show(5)

// COMMAND ----------

newDF.where("count % 2 = 0").show(5)

// COMMAND ----------

// DBTITLE 1,Import a CSV to a DataFrame
val flightDF = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/2015_summary-ebaee.csv")

flightDF.show(5)

// COMMAND ----------

// DBTITLE 1,Sort by count
flightDF
  .orderBy(desc("count"))
  .show(10)

// COMMAND ----------

// DBTITLE 1,Create a temp view to make SQL queries
flightDF.createOrReplaceTempView("flight_tp")

// COMMAND ----------

// DBTITLE 1,Count and sort destinations
val sqlWay = spk.sql("""
  SELECT DEST_COUNTRY_NAME, count(DEST_COUNTRY_NAME)
  FROM flight_tp
  GROUP BY DEST_COUNTRY_NAME
  ORDER BY count(1) DESC
  LIMIT 5
  """)

sqlWay.show(5)

// COMMAND ----------

import org.apache.spark.sql.functions._

val dataframeWay = flightDF
  .groupBy("DEST_COUNTRY_NAME")
  .count()
  .orderBy(desc("count"))

dataframeWay.show(5)

// COMMAND ----------

// DBTITLE 1,Sort by max count
val sqlWay = spk.sql("""
  SELECT DEST_COUNTRY_NAME, max(count) as max_count
  FROM flight_tp
  GROUP BY DEST_COUNTRY_NAME
  ORDER BY max_count DESC
  LIMIT 5
""")

sqlWay.show()

// COMMAND ----------

val sqlWay = spk.sql("""
  SELECT ORIGIN_COUNTRY_NAME, max(count) as max_count
  FROM flight_tp
  GROUP BY ORIGIN_COUNTRY_NAME
  ORDER BY max_count DESC
  LIMIT 5
""")

sqlWay.show()

// COMMAND ----------

// DBTITLE 1,Sort by sum count
val sqlWay = spk.sql("""
  SELECT ORIGIN_COUNTRY_NAME, sum(count) as sum_count
  FROM flight_tp
  GROUP BY ORIGIN_COUNTRY_NAME
  ORDER BY sum_count DESC
  LIMIT 5
""")

sqlWay.show()

// COMMAND ----------

flightDF.groupBy("ORIGIN_COUNTRY_NAME")
  .sum("count")
  .orderBy(desc("sum(count)"))
  .withColumnRenamed("sum(count)", "sum_count")
  .show(5)

// COMMAND ----------

// DBTITLE 1,Show only max count (without any other info)
spk.sql("SELECT max(count) FROM flight_tp").show(1)

// COMMAND ----------

flightDF.groupBy().max("count").show()
