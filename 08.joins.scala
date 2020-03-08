// Inner joins (keep rows with keys that exist in the left and right datasets)
// Outer joins (keep rows with keys in either the left or right datasets)
// Left outer joins (keep rows with keys in the left dataset)
// Right outer joins (keep rows with keys in the right dataset)
// Left semi joins (keep the rows in the left, and only the left, dataset where the keyappears in the right dataset)
// Left anti joins (keep the rows in the left, and only the left, dataset where they do not appear in the right dataset)
// Natural joins (perform a join by implicitly matching the columns between the two datasets with the same names)
// Cross (or Cartesian) joins (match every row in the left dataset with every row in the right dataset)

val person = Seq(
  (0, "Bill Chambers", 0, Seq(100)),
  (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
  (2, "Michael Armbrust", 1, Seq(250, 100))).toDF("id", "name", "graduate_program", "spark_status")

val graduateProgram = Seq(
  (0, "Masters", "School of Information", "UC Berkeley"),
  (2, "Masters", "EECS", "UC Berkeley"),
  (1, "Ph.D.", "EECS", "UC Berkeley")).toDF("id", "degree", "department", "school")

val sparkStatus = Seq(
  (500, "Vice President"),
  (250, "PMC Member"),
  (100, "Contributor")).toDF("id", "status")

person.show()
+---+----------------+----------------+---------------+
| id|            name|graduate_program|   spark_status|
+---+----------------+----------------+---------------+
|  0|   Bill Chambers|               0|          [100]|
|  1|   Matei Zaharia|               1|[500, 250, 100]|
|  2|Michael Armbrust|               1|     [250, 100]|
+---+----------------+----------------+---------------+

graduateProgram.show()
+---+-------+--------------------+-----------+
| id| degree|          department|     school|
+---+-------+--------------------+-----------+
|  0|Masters|School of Informa...|UC Berkeley|
|  2|Masters|                EECS|UC Berkeley|
|  1|  Ph.D.|                EECS|UC Berkeley|
+---+-------+--------------------+-----------+

sparkStatus.show()
+---+--------------+
| id|        status|
+---+--------------+
|500|Vice President|
|250|    PMC Member|
|100|   Contributor|
+---+--------------+

person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")


// ------------------- INNERS JOINS ------------------- 
// the default join
val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
person.join(graduateProgram, joinExpression, "inner").show()

spark.sql("""SELECT * FROM person INNER JOIN graduateProgram 
ON person.graduate_program = graduateProgram.id""").show()
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
| id|            name|graduate_program|   spark_status| id| degree|          department|     school|
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
|  0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
|  2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|  1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+


// ------------------- OUTER JOINS ------------------- 
person.join(graduateProgram, joinExpression, "right_outer").show()

spark.sql("""SELECT * FROM person RIGHT OUTER JOIN graduateProgram
ON person.graduate_program = graduateProgram.id""").show()
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
|  id|            name|graduate_program|   spark_status| id| degree|          department|     school|
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
|   0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
|null|            null|            null|           null|  2|Masters|                EECS|UC Berkeley|
|   2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|   1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+


// ------------------- LEFT SEMI JOINS ------------------- 
graduateProgram.join(person, joinExpression, "left_semi").show()
+---+-------+--------------------+-----------+
| id| degree|          department|     school|
+---+-------+--------------------+-----------+
|  0|Masters|School of Informa...|UC Berkeley|
|  1|  Ph.D.|                EECS|UC Berkeley|
+---+-------+--------------------+-----------+

val gradProgram2 = graduateProgram.union(Seq(
    (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
gradProgram2.createOrReplaceTempView("gradProgram2")

gradProgram2.join(person, joinExpression, "left_semi").show()

spark.sql("""SELECT * FROM gradProgram2 LEFT SEMI JOIN person
ON gradProgram2.id = person.graduate_program""").show()
+---+-------+--------------------+-----------------+
| id| degree|          department|           school|
+---+-------+--------------------+-----------------+
|  0|Masters|School of Informa...|      UC Berkeley|
|  1|  Ph.D.|                EECS|      UC Berkeley|
|  0|Masters|      Duplicated Row|Duplicated School|
+---+-------+--------------------+-----------------+


// ------------------- LEFT ANTI JOINS, the opposite of left semi joins ------------------- 
graduateProgram.join(person, joinExpression, "left_anti").show()

spark.sql("""SELECT * FROM graduateProgram LEFT ANTI JOIN person
ON graduateProgram.id = person.graduate_program""").show()
+---+-------+----------+-----------+
| id| degree|department|     school|
+---+-------+----------+-----------+
|  2|Masters|      EECS|UC Berkeley|
+---+-------+----------+-----------+


// ------------------- NATURAL JOINS ------------------- 
// make implicit guesses at the columns on which you would like to join : dangerous
spark.sql("SELECT * FROM graduateProgram NATURAL JOIN person").show()
+---+-------+--------------------+-----------+----------------+----------------+---------------+
| id| degree|          department|     school|            name|graduate_program|   spark_status|
+---+-------+--------------------+-----------+----------------+----------------+---------------+
|  0|Masters|School of Informa...|UC Berkeley|   Bill Chambers|               0|          [100]|
|  2|Masters|                EECS|UC Berkeley|Michael Armbrust|               1|     [250, 100]|
|  1|  Ph.D.|                EECS|UC Berkeley|   Matei Zaharia|               1|[500, 250, 100]|
+---+-------+--------------------+-----------+----------------+----------------+---------------+


// ------------------- CROSS (Cartesian) JOINS ------------------- 
graduateProgram.join(person, joinExpression, "cross").show()

spark.sql("""SELECT * FROM graduateProgram CROSS JOIN person
ON graduateProgram.id = person.graduate_program""").show()
+---+-------+--------------------+-----------+---+----------------+----------------+---------------+
| id| degree|          department|     school| id|            name|graduate_program|   spark_status|
+---+-------+--------------------+-----------+---+----------------+----------------+---------------+
|  0|Masters|School of Informa...|UC Berkeley|  0|   Bill Chambers|               0|          [100]|
|  1|  Ph.D.|                EECS|UC Berkeley|  2|Michael Armbrust|               1|     [250, 100]|
|  1|  Ph.D.|                EECS|UC Berkeley|  1|   Matei Zaharia|               1|[500, 250, 100]|
+---+-------+--------------------+-----------+---+----------------+----------------+---------------+


person.crossJoin(graduateProgram).show()
spark.sql("SELECT * FROM graduateProgram CROSS JOIN person").show()
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
| id|            name|graduate_program|   spark_status| id| degree|          department|     school|
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
|  0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
|  1|   Matei Zaharia|               1|[500, 250, 100]|  0|Masters|School of Informa...|UC Berkeley|
|  2|Michael Armbrust|               1|     [250, 100]|  0|Masters|School of Informa...|UC Berkeley|
|  0|   Bill Chambers|               0|          [100]|  2|Masters|                EECS|UC Berkeley|
|  1|   Matei Zaharia|               1|[500, 250, 100]|  2|Masters|                EECS|UC Berkeley|
|  2|Michael Armbrust|               1|     [250, 100]|  2|Masters|                EECS|UC Berkeley|
|  0|   Bill Chambers|               0|          [100]|  1|  Ph.D.|                EECS|UC Berkeley|
|  1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|  2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+


// ------------------- CHALLENGES WHEN USING JOINS ------------------- 
// Joins on Complex Types
import org.apache.spark.sql.functions._

person.withColumnRenamed("id", "personId")
  .join(sparkStatus, expr("array_contains(spark_status, id)")).show()

spark.sql("""SELECT * FROM (select id as personId, name, graduate_program, spark_status FROM person)
INNER JOIN sparkStatus ON array_contains(spark_status, id)""").show()
+--------+----------------+----------------+---------------+---+--------------+
|personId|            name|graduate_program|   spark_status| id|        status|
+--------+----------------+----------------+---------------+---+--------------+
|       0|   Bill Chambers|               0|          [100]|100|   Contributor|
|       1|   Matei Zaharia|               1|[500, 250, 100]|500|Vice President|
|       1|   Matei Zaharia|               1|[500, 250, 100]|250|    PMC Member|
|       1|   Matei Zaharia|               1|[500, 250, 100]|100|   Contributor|
|       2|Michael Armbrust|               1|     [250, 100]|250|    PMC Member|
|       2|Michael Armbrust|               1|     [250, 100]|100|   Contributor|
+--------+----------------+----------------+---------------+---+--------------+


// Handling Duplicate Column Names
val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
val joinExpr = gradProgramDupe.col("graduate_program") === person.col("graduate_program")
+--------+----------------+----------------+---------------+---+--------------+
|personId|            name|graduate_program|   spark_status| id|        status|
+--------+----------------+----------------+---------------+---+--------------+
|       0|   Bill Chambers|               0|          [100]|100|   Contributor|
|       1|   Matei Zaharia|               1|[500, 250, 100]|500|Vice President|
|       1|   Matei Zaharia|               1|[500, 250, 100]|250|    PMC Member|
|       1|   Matei Zaharia|               1|[500, 250, 100]|100|   Contributor|
|       2|Michael Armbrust|               1|     [250, 100]|250|    PMC Member|
|       2|Michael Armbrust|               1|     [250, 100]|100|   Contributor|
+--------+----------------+----------------+---------------+---+--------------+


// ------------------- HOW SPARK PERFORMS JOINS ------------------- 
val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
person.join(graduateProgram, joinExpr).explain()
== Physical Plan ==
*(1) BroadcastHashJoin [graduate_program#13], [id#28], Inner, BuildLeft
:- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[2, int, false] as bigint))), [id=#1104]
:  +- LocalTableScan [id#11, name#12, graduate_program#13, spark_status#14]
+- LocalTableScan [id#28, degree#29, department#30, school#31]
joinExpr: org.apache.spark.sql.Column = (graduate_program = id)


val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
person.join(broadcast(graduateProgram), joinExpr).explain()
== Physical Plan ==
*(1) BroadcastHashJoin [graduate_program#13], [id#28], Inner, BuildLeft
:- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[2, int, false] as bigint))), [id=#1104]
:  +- LocalTableScan [id#11, name#12, graduate_program#13, spark_status#14]
+- LocalTableScan [id#28, degree#29, department#30, school#31]
joinExpr: org.apache.spark.sql.Column = (graduate_program = id)