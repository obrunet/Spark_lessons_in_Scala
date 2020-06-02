// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("app name").getOrCreate()

val df = spark.read.format("csv")
  .option("header", "true")
  .option("infSchema", "true")
  .load("/FileStore/tables/online_retail_dataset-92e8e.csv")
  .coalesce(5)

df.cache()
df.createOrReplaceTempView("dfTable")

df.show()

// COMMAND ----------

df.count()

// COMMAND ----------

import org.apache.spark.sql.functions._

df.select(count("StockCode")).show()

// COMMAND ----------

spark.sql("""SELECT count("StockCode") FROM dfTable""").show()

// COMMAND ----------

df.select(countDistinct("StockCode")).show()

// COMMAND ----------

spark.sql("SELECT COUNT(DISTINCT *) FROM DFTABLE").show()

// COMMAND ----------

df.select(approx_count_distinct("StockCode", 0.1)).show()

// COMMAND ----------

spark.sql("SELECT approx_count_distinct(StockCode, 0.1) FROM DFTABLE").show()

// COMMAND ----------

import org.apache.spark.sql.functions._
df.select(first("StockCode"), last("StockCode")).show()

// COMMAND ----------

spark.sql("SELECT first(StockCode), last(StockCode) FROM dfTable").show()

// COMMAND ----------

df.select(min("Quantity"), max("Quantity")).show()

// COMMAND ----------

spark.sql("SELECT min(Quantity), max(Quantity) FROM dfTable").show()

// COMMAND ----------

df.select(sum("Quantity")).show()

// COMMAND ----------

spark.sql("SELECT sum(Quantity) FROM dfTable").show()

// COMMAND ----------

df.select(sumDistinct("Quantity")).show()

// COMMAND ----------

spark.sql("SELECT SUM(DISTINCT(Quantity)) FROM dfTable").show()

// COMMAND ----------

df.select(
  sum("Quantity").alias("total_purchases"),
  count("Quantity").alias("total_transactions"),
  avg("Quantity").alias("avg_purchases"),
  expr("mean(Quantity)").alias("mean_purchases"))
.selectExpr("total_purchases/total_transactions", "avg_purchases", "mean_purchases")
.show()

// COMMAND ----------

df.select(var_pop("Quantity"), var_samp("Quantity"),
  stddev_pop("Quantity"), stddev_samp("Quantity")).show()

// COMMAND ----------

spark.sql("""
SELECT var_pop(Quantity), var_samp(Quantity),
stddev_pop(Quantity), stddev_samp(Quantity)
FROM dfTable
""").show()

// COMMAND ----------

df.select(skewness("Quantity"), kurtosis("Quantity")).show()

// COMMAND ----------

spark.sql("SELECT skewness(Quantity), kurtosis(Quantity) FROM dfTable").show()

// COMMAND ----------

df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
  covar_pop("InvoiceNo", "Quantity")).show()

// COMMAND ----------

spark.sql("""
SELECT corr(InvoiceNo, Quantity), covar_samp(InvoiceNo, Quantity),
covar_pop(InvoiceNo, Quantity)
FROM dfTable""").show()

// COMMAND ----------

df.agg(collect_set("Country"), collect_list("Country")).show()

// COMMAND ----------

spark.sql("SELECT collect_set(Country), collect_list(Country) FROM dfTable").show()

// COMMAND ----------


