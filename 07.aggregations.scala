val df = spark.read
  .format("csv")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("FileStore/tables/online_retail_dataset-92e8e.csv")
  .coalesce(5)

df.createOrReplaceTempView("dfTable")
+---------+---------+--------------------+--------+--------------+---------+----------+--------------+
|InvoiceNo|StockCode|         Description|Quantity|   InvoiceDate|UnitPrice|CustomerID|       Country|
+---------+---------+--------------------+--------+--------------+---------+----------+--------------+
|   536365|   85123A|WHITE HANGING HEA...|       6|12/1/2010 8:26|     2.55|     17850|United Kingdom|
|   536365|    71053| WHITE METAL LANTERN|       6|12/1/2010 8:26|     3.39|     17850|United Kingdom|
+---------+---------+--------------------+--------+--------------+---------+----------+--------------+
df.count()
res2: Long = 541909

// ------------ AGGREGATION FUNCTIONS ------------ 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
df.select(count("StockCode")).show()
spark.sql("SELECT count(StockCode) FROM dfTable").show()
+----------------+
|count(StockCode)|
+----------------+
|          541909|
+----------------+

df.select(countDistinct("StockCode")).show()
spark.sql("SELECT count(distinct(StockCode)) FROM dfTable").show()
+-------------------------+
|count(DISTINCT StockCode)|
+-------------------------+
|                     4070|
+-------------------------+

df.select(approx_count_distinct("StockCode", 0.1)).show()
spark.sql("SELECT approx_count_distinct(StockCode) FROM dfTable").show()
+--------------------------------+
|approx_count_distinct(StockCode)|
+--------------------------------+
|                            3364|
+--------------------------------+

df.select(first("StockCode"), last("StockCode")).show()
spark.sql("SELECT first(StockCode), last(StockCode) FrOM dfTable").show()
+-----------------------+----------------------+
|first(StockCode, false)|last(StockCode, false)|
+-----------------------+----------------------+
|                 85123A|                 22138|
+-----------------------+----------------------+

df.select(min("Quantity"), max("Quantity")).show()
spark.sql("SELECT min(Quantity), max(Quantity) FrOM dfTable").show()
+-------------+-------------+
|min(Quantity)|max(Quantity)|
+-------------+-------------+
|       -80995|        80995|
+-------------+-------------+

df.select(sum("Quantity")).show()
spark.sql("SELECT sum(Quantity) FrOM dfTable").show()
+-------------+
|sum(Quantity)|
+-------------+
|      5176450|
+-------------+

df.select(sumDistinct("Quantity")).show()
//spark.sql("SELECT sumDisctint(Quantity) FrOM dfTable").show() ----------- bug
+----------------------+
|sum(DISTINCT Quantity)|
+----------------------+
|                 29310|
+----------------------+

df.select(avg("Quantity")).show()
spark.sql("SELECT avg(Quantity) FROM dfTable").show()
+----------------+
|   avg(Quantity)|
+----------------+
|9.55224954743324|
+----------------+

df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))
    .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()
+--------------------------------------+----------------+----------------+
|(total_purchases / total_transactions)|   avg_purchases|  mean_purchases|
+--------------------------------------+----------------+----------------+
|                      9.55224954743324|9.55224954743324|9.55224954743324|
+--------------------------------------+----------------+----------------+


df.select(var_pop("Quantity"), var_samp("Quantity"), stddev_pop("Quantity"), stddev_samp("Quantity")).show()
spark.sql("""
    SELECT var_pop(Quantity), var_samp(Quantity), stddev_pop(Quantity), stddev_samp(Quantity)
    FROM dfTable""").show()
+------------------+------------------+--------------------+---------------------+
| var_pop(Quantity)|var_samp(Quantity)|stddev_pop(Quantity)|stddev_samp(Quantity)|
+------------------+------------------+--------------------+---------------------+
|47559.303646609056|47559.391409298754|  218.08095663447796|   218.08115785023418|
+------------------+------------------+--------------------+---------------------+

// Variance and Standard Deviation
df.select(var_pop("Quantity"), var_samp("Quantity"),
    stddev_pop("Quantity"), stddev_samp("Quantity")).show()
spark.sql("""SELECT var_pop(Quantity), var_samp(Quantity),
stddev_pop(Quantity), stddev_samp(Quantity)
FROM dfTable""").show()
+------------------+------------------+--------------------+---------------------+
| var_pop(Quantity)|var_samp(Quantity)|stddev_pop(Quantity)|stddev_samp(Quantity)|
+------------------+------------------+--------------------+---------------------+
|47559.303646609056|47559.391409298754|  218.08095663447796|   218.08115785023418|
+------------------+------------------+--------------------+---------------------+

// skewness and kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()
spark.sql("SELECT skewness(Quantity), kurtosis(Quantity) FROM dfTable").show()
+-------------------+------------------+
| skewness(Quantity)|kurtosis(Quantity)|
+-------------------+------------------+
|-0.2640755761052562|119768.05495536952|
+-------------------+------------------+

// Covariance and Correlation
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
  covar_pop("InvoiceNo", "Quantity")).show()

spark.sql("""
SELECT corr(InvoiceNo, Quantity), covar_samp(InvoiceNo, Quantity),
covar_pop(InvoiceNo, Quantity)
FROM dfTable""").show()
+-------------------------+-------------------------------+------------------------------+
|corr(InvoiceNo, Quantity)|covar_samp(InvoiceNo, Quantity)|covar_pop(InvoiceNo, Quantity)|
+-------------------------+-------------------------------+------------------------------+
|     4.912186085635685E-4|             1052.7280543902734|            1052.7260778741693|
+-------------------------+-------------------------------+------------------------------+

// Aggregating to Complex Types
df.agg(collect_set("Country"), collect_list("Country")).show()
spark.sql("SELECT collect_set(Country), collect_set(Country) FROM dfTable").show()
+--------------------+---------------------+
|collect_set(Country)|collect_list(Country)|
+--------------------+---------------------+
|[Portugal, Italy,...| [United Kingdom, ...|
+--------------------+---------------------+

// ------------ GROUPING ------------ 
df.groupBy("InvoiceNo", "CustomerId").count().show()
spark.sql("SELECT count(*) FROM dfTable GROUP BY InvoiceNo, CustomerId").show()
+---------+----------+-----+
|InvoiceNo|CustomerId|count|
+---------+----------+-----+
|   536846|     14573|   76|
|   537026|     12395|   12|
...

// Grouping with Expressions
df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()
+---------+----+---------------+
|InvoiceNo|quan|count(Quantity)|
+---------+----+---------------+
|   536596|   6|              6|
|   536938|  14|             14|
...

// Grouping with Maps
df.groupBy("InvoiceNo").agg("Quantity"->"avg", "Quantity"->"stddev_pop").show()
spark.sql("SELECT avg(Quantity), stddev_pop(Quantity), InvoiceNo FROM dfTable GROUP BY InvoiceNo").show()
+---------+------------------+--------------------+
|InvoiceNo|     avg(Quantity)|stddev_pop(Quantity)|
+---------+------------------+--------------------+
|   536596|               1.5|  1.1180339887498947|
|   536938|33.142857142857146|  20.698023172885524|
...

// ------------ WINDOW FUNCTIONS ------------ 
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
+----------+----------+--------+------------+-----------------+-------------------+
|CustomerId|      date|Quantity|quantityRank|quantityDenseRank|maxPurchaseQuantity|
+----------+----------+--------+------------+-----------------+-------------------+
|     12346|2011-01-18|   74215|           1|                1|              74215|
|     12346|2011-01-18|  -74215|           2|                2|              74215|
|     12347|2010-12-07|      36|           1|                1|                 36|
|     12347|2010-12-07|      30|           2|                2|                 36|
...

// ------------ GROUPING SETS ------------ 
val dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

spark.sql("""SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
  GROUP BY customerId, stockCode
  ORDER BY CustomerId DESC, stockCode DESC""").show()
+----------+---------+-------------+
|CustomerId|stockCode|sum(Quantity)|
+----------+---------+-------------+
|     18287|    85173|           48|
|     18287|   85040A|           48|
|     18287|   85039B|          120|
|     18287|   85039A|           96|
...

// Exact same thing by using a grouping set
// If you do not filter-out null values, you will get incorrect results (also for cubes, rollups)
spark.sql("""SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
ORDER BY CustomerId DESC, stockCode DESC""").show()
+----------+---------+-------------+
|customerId|stockCode|sum(Quantity)|
+----------+---------+-------------+
|     18287|    85173|           48|
|     18287|   85040A|           48|
|     18287|   85039B|          120|
|     18287|   85039A|           96|

// Rollups
val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
    .orderBy("Date")

rolledUpDF.where("Country IS NULL").show()
+----------+-------+--------------+
|      Date|Country|total_quantity|
+----------+-------+--------------+
|      null|   null|       5176450|
|2010-12-01|   null|         26814|
|2010-12-02|   null|         21023|
|2010-12-03|   null|         14830|

// Cube (ie rollup to a level deeper)
dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
    .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()
+----+--------------------+-------------+
|Date|             Country|sum(Quantity)|
+----+--------------------+-------------+
|null|            Portugal|        16180|
|null|           Australia|        83653|
|null|                 RSA|          352|
|null|                null|      5176450|

// Pivot
val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`").show()
+----------+-----------------+
|      date|USA_sum(Quantity)|
+----------+-----------------+
|2011-12-06|             null|
|2011-12-09|             null|
|2011-12-08|             -196|
|2011-12-07|             null|
+----------+-----------------+

// ------------ USER-DEFINED AGGREGATION FUNCTIONS ------------ 

import org.apache.spark.sql.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._