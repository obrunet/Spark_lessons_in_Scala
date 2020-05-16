// Databricks notebook source
// ----------- Read API Structure

//The core structure for reading data is as follows:
//DataFrameReader.format(...).option("key", "value").schema(...).load()

spark.read.format("csv")
  .option("mode", "FAILFAST")
  .option("inferSchema", "true")
  .option("path", "path/to/file(s)")
  .schema(someSchema)
  .load()

// COMMAND ----------

// ----------- Write API Structure

// The core structure for writing data is as follows:
// DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).save()
dataframe.write.format("csv")
  .option("mode", "OVERWRITE")
  .option("dateFormat", "yyyy-MM-dd")
  .option("path", "path/to/file(s)")
  .save()

// COMMAND ----------

// ----------- CSV Files
spark.read.format("csv")
  .option("header", "true")
  .option("mode", "FAILFAST")
  .option("inferSchema", "true")
  .load("some/path/to/file.csv")

// COMMAND ----------

import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()


import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

val myManualSchema =  new StructType(Array(
                      new StructField("DEST_COUNTRY_NAME", StringType, true),
                      new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
                      new StructField("count", LongType, false)
                      ))
val csvFile = spark.read.format("csv")
  .option("header", "true")
  .option("mode", "FAILFAST")
  .schema(myManualSchema)
  .load("/FileStore/tables/2015_summary-ebaee.csv")

csvFile.show(5)

// COMMAND ----------

csvFile.write.format("csv").mode("overwrite").option("sep", "\t")
    .save("/tmp/my-tsv-file.tsv")

// When you list the destination directory, you can see that my-tsv-file is actually a folder with
// numerous files within it: This actually reflects the number of partitions in our DataFrame

// COMMAND ----------

// ----------- JSON Files
spark.read.format("json").option("mode", "FAILFAST").schema(myManualSchema)
.load("/FileStore/tables/2015_summary-ebaee.json").show(5)

// COMMAND ----------

csvFile.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")

// COMMAND ----------

// ----------- Parquet Files
// specify Parquet as the read format:
spark.read.format("parquet")

spark.read.format("parquet")
.load("/FileStore/tables/2010_summary-506d8.parquet").show(5)

// COMMAND ----------

csvFile.write.format("parquet").mode("overwrite")
  .save("/tmp/my-parquet-file.parquet")

// COMMAND ----------

// ----------- ORC Files

spark.read.format("orc").load("/FileStore/tables/2010_summary-506d8.orc").show(5)

// COMMAND ----------

csvFile.write.format("orc").mode("overwrite").save("/tmp/my-json-file.orc")

// COMMAND ----------

// ----------- SQL Databases

val driver = "org.sqlite.JDBC"
val path = "/data/flight-data/jdbc/my-sqlite.db"
val url = s"jdbc:sqlite:/${path}"
val tablename = "flight_info"

val dbDataFrame = spark.read.format("jdbc").option("url", url)
  .option("dbtable", tablename).option("driver", driver).load()
// /FileStore/tables/my_sqlite-e9c7d.db

// COMMAND ----------

val pgDF = spark.read
  .format("jdbc")
  .option("driver", "org.postgresql.Driver")
  .option("url", "jdbc:postgresql://database_server")
  .option("dbtable", "schema.tablename")
  .option("user", "username").option("password","my-secret-password").load()

// COMMAND ----------

dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show(5)

// COMMAND ----------

// Query Pushdown

val pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
  AS flight_info"""
val dbDataFrame = spark.read.format("jdbc")
  .option("url", url).option("dbtable", pushdownQuery).option("driver", driver)
  .load()

// COMMAND ----------

// Reading from databases in parallel
val dbDataFrame = spark.read.format("jdbc")
  .option("url", url).option("dbtable", tablename).option("driver", driver)
  .option("numPartitions", 10).load()

// COMMAND ----------

// Writing to SQL Databases
val newPath = "jdbc:sqlite://tmp/my-sqlite.db"
csvFile.write.mode("overwrite").jdbc(newPath, tablename, props)

// COMMAND ----------

csvFile.write.mode("append").jdbc(newPath, tablename, props)

// COMMAND ----------

// ----------- Text Files
// Each line in the file becomes a record in the DF
spark.read.textFile("/FileStore/tables/2015_summary-ebaee.csv")
  .selectExpr("split(value, ',') as rows").show()


// COMMAND ----------

csvFile.limit(10).select("DEST_COUNTRY_NAME", "count")
  .write.partitionBy("count").text("/tmp/five-csv-files2.csv")
