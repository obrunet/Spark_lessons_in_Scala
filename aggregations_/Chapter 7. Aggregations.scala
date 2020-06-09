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

df.groupBy("InvoiceNo", "CustomerId").count().show()

// COMMAND ----------

spark.sql("""
SELECT InvoiceNo, CustomerId, count(*) FROM dfTable GROUP BY InvoiceNo, CustomerId
""").show()

// COMMAND ----------

df.groupBy("InvoiceNo").agg(
  //count("Quantity").alias("quan"),
  expr("count(Quantity)")).show()

// COMMAND ----------

df.where("InvoiceNo = 541432").show()

// COMMAND ----------

df.groupBy("InvoiceNo").agg(expr("sum(Quantity)")).show()

// COMMAND ----------

df.where("InvoiceNo = 541432").agg(expr("sum(Quantity)")).show()

// COMMAND ----------

df.where("InvoiceNo = 541432 or InvoiceNo = 536596").agg(expr("sum(Quantity)")).show()

// COMMAND ----------

var dfTest = df.groupBy("InvoiceNo").agg(expr("sum(Quantity)"))
dfTest.where("InvoiceNo = 541432 or InvoiceNo = 536596").show()

// COMMAND ----------

df.groupBy("InvoiceNo").agg("Quantity"->"avg", "Quantity"->"stddev_pop").show()

// COMMAND ----------

spark.sql("""
SELECT avg(Quantity), stddev_pop(Quantity), InvoiceNo FROM dfTable
GROUP BY InvoiceNo
""").show()

// COMMAND ----------

//Window Functions

// COMMAND ----------

val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")

// COMMAND ----------

dfWithDate.show()

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

val windowSpec = Window
  .partitionBy("CustomerId", "date")
  .orderBy(col("Quantity").desc)
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

// COMMAND ----------

import org.apache.spark.sql.functions.max
val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

// COMMAND ----------

val purchaseDenseRank = dense_rank().over(windowSpec)
val purchaseRank = rank().over(windowSpec)

// COMMAND ----------

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId").select(
  col("CustomerId"),
  col("date"),
  col("Quantity"),
  purchaseRank.alias("quantityRank"),
  purchaseDenseRank.alias("quantityDenseRank"),
  maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()

// COMMAND ----------

spark.sql("""
SELECT CustomerId, date, Quantity, rank(Quantity) OVER (PARTITION BY CustomerId, date
ORDER BY Quantity DESC NULLS LAST ROWS BETWEEN
UNBOUNDED PRECEDING AND CURRENT ROW) as rank,
dense_rank(Quantity) OVER (PARTITION BY CustomerId, date
ORDER BY Quantity DESC NULLS LAST
ROWS BETWEEN
UNBOUNDED PRECEDING AND
CURRENT ROW) as dRank,
max(Quantity) OVER (PARTITION BY CustomerId, date
ORDER BY Quantity DESC NULLS LAST
ROWS BETWEEN
UNBOUNDED PRECEDING AND
CURRENT ROW) as maxPurchase
FROM dfWithDate WHERE CustomerId IS NOT NULL ORDER BY CustomerId
""").show()

// COMMAND ----------

// Grouping Sets

// COMMAND ----------

val dfNoNull = dfWithDate.drop()

dfNoNull.createOrReplaceTempView("dfNoNull")

// COMMAND ----------

dfNoNull.count()

// COMMAND ----------

// Grouping Sets

// COMMAND ----------

spark.sql("""
SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode
ORDER BY CustomerId DESC, stockCode DESC""").show()

// COMMAND ----------

// if you also want to include the total number of items, regardless of
// customer or stock code? With a conventional group-by statement, this would be impossible.
spark.sql("""
SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode),())
ORDER BY CustomerId DESC, stockCode DESC
""").show()

// COMMAND ----------

// rollups
// is a multidimensional aggregation that performs a variety of group-by style calculations for us
val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
    .orderBy("Date")

rolledUpDF.show()

// COMMAND ----------

rolledUpDF.where("Country IS NULL").show() // same thing with Date

// COMMAND ----------

// Cube - takes the rollup to a level deeper. Rather than treating elements hierarchically, 
// a cube does the same thing across all dimensions

dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
  .select("Date", "Country", "sum(Quantity)").orderBy(desc("Date")).show()

// COMMAND ----------

// Grouping Metadata
dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
    .orderBy(expr("grouping_id()").desc)
    .show()

// COMMAND ----------

// Pivot
// make it possible for you to convert a row into a column
val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()

// /!\ Problem here
pivoted.where("date > '2011-12-05'").select("date", "sum(UnitPrice),").show()

// COMMAND ----------

// ----------- User-Defined Aggregation Functions UDAFs
// a way for users to define their own agg func based on custom formulae / business rules
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


class BoolAnd extends UserDefinedAggregateFunction {
  
  def inputSchema: org.apache.spark.sql.types.StructType =
  StructType(StructField("value", BooleanType) :: Nil)
  
  def bufferSchema: StructType = StructType(
  StructField("result", BooleanType) :: Nil
  )
  def dataType: DataType = BooleanType
  
  def deterministic: Boolean = true
  
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = true
  }
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
  }
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
  }
  def evaluate(buffer: Row): Any = {
    buffer(0)
  }
}

val ba = new BoolAnd
spark.udf.register("booland", ba)

import org.apache.spark.sql.functions._
spark.range(1)
  .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
  .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
  .select(ba(col("t")), expr("booland(f)"))
  .show()

// COMMAND ----------


