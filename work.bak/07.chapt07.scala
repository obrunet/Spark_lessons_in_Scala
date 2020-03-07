// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
  .appName("app name")
  .getOrCreate()

// COMMAND ----------

val df = spark.read
  .format("csv")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("FileStore/tables/online_retail_dataset-92e8e.csv")
  .coalesce(5)

df.createOrReplaceTempView("dfTable")

df.show(2)

// COMMAND ----------

df.count()

// COMMAND ----------

// ------------ Aggregation Functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
df.select(count("StockCode")).show()
spark.sql("SELECT count(StockCode) FROM dfTable").show()

// COMMAND ----------

df.select(countDistinct("StockCode")).show()
spark.sql("SELECT count(distinct(StockCode)) FROM dfTable").show()

// COMMAND ----------

df.select(approx_count_distinct("StockCode", 0.1)).show()
spark.sql("SELECT approx_count_distinct(StockCode) FROM dfTable").show()

// COMMAND ----------

df.select(first("StockCode"), last("StockCode")).show()
spark.sql("SELECT first(StockCode), last(StockCode) FrOM dfTable").show()

// COMMAND ----------

df.select(min("Quantity"), max("Quantity")).show()
spark.sql("SELECT min(Quantity), max(Quantity) FrOM dfTable").show()

// COMMAND ----------

df.select(sum("Quantity")).show()
spark.sql("SELECT sum(Quantity) FrOM dfTable").show()

// COMMAND ----------

df.select(sumDistinct("Quantity")).show()
//spark.sql("SELECT sumDisctint(Quantity) FrOM dfTable").show() ----------- bug

// COMMAND ----------

df.select(avg("Quantity")).show()
spark.sql("SELECT avg(Quantity) FROM dfTable").show()

// COMMAND ----------

df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))
    .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()


// COMMAND ----------

df.select(var_pop("Quantity"), var_samp("Quantity"), stddev_pop("Quantity"), stddev_samp("Quantity")).show()
spark.sql("""
    SELECT var_pop(Quantity), var_samp(Quantity), stddev_pop(Quantity), stddev_samp(Quantity)
    FROM dfTable""").show()

// COMMAND ----------

// Variance and Standard Deviation
df.select(var_pop("Quantity"), var_samp("Quantity"),
    stddev_pop("Quantity"), stddev_samp("Quantity")).show()
spark.sql("""SELECT var_pop(Quantity), var_samp(Quantity),
stddev_pop(Quantity), stddev_samp(Quantity)
FROM dfTable""").show()

// COMMAND ----------

// skewness and kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()
spark.sql("SELECT skewness(Quantity), kurtosis(Quantity) FROM dfTable").show()

// COMMAND ----------

// Covariance and Correlation
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
  covar_pop("InvoiceNo", "Quantity")).show()

spark.sql("""
SELECT corr(InvoiceNo, Quantity), covar_samp(InvoiceNo, Quantity),
covar_pop(InvoiceNo, Quantity)
FROM dfTable""").show()

// COMMAND ----------

// Aggregating to Complex Types
df.agg(collect_set("Country"), collect_list("Country")).show()
spark.sql("SELECT collect_set(Country), collect_set(Country) FROM dfTable").show()

// COMMAND ----------

// -------------- Grouping
df.groupBy("InvoiceNo", "CustomerId").count().show()
spark.sql("SELECT count(*) FROM dfTable GROUP BY InvoiceNo, CustomerId").show()

// COMMAND ----------

// Grouping with Expressions
df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()

// COMMAND ----------

// Grouping with Maps
df.groupBy("InvoiceNo").agg("Quantity"->"avg", "Quantity"->"stddev_pop").show()
spark.sql("SELECT avg(Quantity), stddev_pop(Quantity), InvoiceNo FROM dfTable GROUP BY InvoiceNo").show()

// COMMAND ----------

// ----------- Window Functions
import org.apache.spark.sql.expressions.Window

val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")

val windowSpec = Window
    .partitionBy("CustomerId", "date")
    .orderBy(col("Quantity").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
val purchaseDenseRank = dense_rank().over(windowSpec)
val purchaseRank = rank().over(windowSpec)

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId").select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()

// COMMAND ----------

// ----------- Grouping Sets
val dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

spark.sql("""SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
  GROUP BY customerId, stockCode
  ORDER BY CustomerId DESC, stockCode DESC""").show()

// COMMAND ----------

// Exact same thing by using a grouping set
// If you do not filter-out null values, you will get incorrect results (also for cubes, rollups)
spark.sql("""SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
ORDER BY CustomerId DESC, stockCode DESC""").show()

// COMMAND ----------

// Rollups
val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
    .orderBy("Date")

// COMMAND ----------

rolledUpDF.where("Country IS NULL").show()

// COMMAND ----------

// Cube (ie rollup to a level deeper)
dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
    .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()

// COMMAND ----------

// Pivot
val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`").show()

// COMMAND ----------

// -------------- User-Defined Aggregation Functions
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


