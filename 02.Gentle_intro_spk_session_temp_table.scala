// ------- SPARK SESSION ------- 
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder
  .appName("Simple Application")
  .getOrCreate()
import org.apache.spark.sql.SparkSession
spark: org.apache.spark.sql.SparkSession = 
	org.apache.spark.sql.SparkSession@497e93b0


// Create DF from range  
val df = spark.range(100).toDF("numbers")
df.show(4)
+-------+
|numbers|
+-------+
|      0|
|      1|
|      2|
|      3|
+-------+


// Divisible by 2
df.where("numbers % 2 = 0").show(4)
+-------+
|numbers|
+-------+
|      0|
|      2|
|      4|
|      6|
+-------+


// ------- READ CSV ------- 
val flightDF = spark
  .read
  .option("inferSchema", "True")
  .option("header", "True")
  .format("csv")
  .load("/FileStore/tables/2015_summary-ebaee.csv")
flightDF.show(2)
+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Romania|   15|
|    United States|            Croatia|    1|
+-----------------+-------------------+-----+


// show first 3
flightDF.take(3)
Array[org.apache.spark.sql.Row] = Array([United States,Romania,15], 
	[United States,Croatia,1], [United States,Ireland,344])


// sort by count
import org.apache.spark.sql.functions._
flightDF.sort(desc("count")).show(3)
+-----------------+-------------------+------+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME| count|
+-----------------+-------------------+------+
|    United States|      United States|370002|
|    United States|             Canada|  8483|
|           Canada|      United States|  8399|
+-----------------+-------------------+------+


// physical plan
flightDF.sort(desc("count")).explain
== Physical Plan ==
Sort [count#26 DESC NULLS LAST], true, 0
+- Exchange rangepartitioning(count#26 DESC NULLS LAST, 200), [id=#169]
   +- *(1) FileScan csv [DEST_COUNTRY_NAME#24,ORIGIN_COUNTRY_


// ------- TEMP TABLE ------- 
// query it using pure SQL. No performance difference
flightDF.createOrReplaceTempView("dfTable")


// count nb of dest countries
val sqlWay = spark.sql("""
  SELECT DEST_COUNTRY_NAME, count(DEST_COUNTRY_NAME)
  FROM dfTable
  GROUP BY DEST_COUNTRY_NAME
  ORDER BY count(1) DESC
  LIMIT 5
  """)
sqlWay.show(2)

val dfWay = flightDF
  .groupBy('DEST_COUNTRY_NAME)
  .count()
  .orderBy(desc("count"))
dfWay.show(2)
+-----------------+------------------------+
|DEST_COUNTRY_NAME|count(DEST_COUNTRY_NAME)|
+-----------------+------------------------+
|    United States|                     125|
|         Paraguay|                       1|
+-----------------+------------------------+


// return the max count 
spark.sql("SELECT max(count) from dfTable").take(1)(0)
org.apache.spark.sql.Row = [370002]

flightDF.select(max("count")).take(1)
Array[org.apache.spark.sql.Row] = Array([370002])


// return the sum of counts and order
spark.sql("""
  SELECT DEST_COUNTRY_NAME, sum(count) as sum_count
  FROM dfTable
  GROUP BY DEST_COUNTRY_NAME
  ORDER BY sum_count DESC
  LIMIT 5
  """).show(5)
org.apache.spark.sql.Row = [370002]

flightDF
  .groupBy("DEST_COUNTRY_NAME")
  .sum("count")
  .withColumnRenamed("sum(count)", "destination_total")
  .sort(desc("destination_total"))
  .limit(5)
  .show()

+-----------------+---------+
|DEST_COUNTRY_NAME|sum_count|
+-----------------+---------+
|    United States|   411352|
|           Canada|     8399|
|           Mexico|     7140|
|   United Kingdom|     2025|
|            Japan|     1548|
+-----------------+---------+