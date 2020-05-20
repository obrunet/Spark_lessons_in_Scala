// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("dsfrsdr").getOrCreate()

// COMMAND ----------

val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")

val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EE__CS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")

val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")

// COMMAND ----------

person.show()

// COMMAND ----------

graduateProgram.show()

// COMMAND ----------

sparkStatus.show()

// COMMAND ----------

import org.apache.spark.sql.functions._

person.createOrReplaceTempView("personTable")
graduateProgram.createOrReplaceTempView("graduateTable")
sparkStatus.createOrReplaceTempView("statusTable")

// COMMAND ----------

spark.sql("""
SELECT *
FROM personTable INNER JOIN graduateTable ON personTable.graduate_program = graduateTable.id
""").show()

// COMMAND ----------

val joinExpr = person.col("graduate_program") === graduateProgram.col("id")

person.join(graduateProgram, joinExpr, "inner").show()

// COMMAND ----------

spark.sql("""
SELECT *
FROM personTable RIGHT OUTER JOIN graduateTable ON personTable.graduate_program = graduateTable.id
""").show()

// COMMAND ----------

val joinExpr = person.col("graduate_program") === graduateProgram.col("id")

person.join(graduateProgram, joinExpr, "right_outer").show()

// COMMAND ----------

graduateProgram.join(person, joinExpr, "left_semi").show()

// COMMAND ----------

spark.sql("""
SELECT *
FROM graduateTable LEFT SEMI JOIN personTable ON personTable.graduate_program = graduateTable.id
""").show()

// COMMAND ----------


