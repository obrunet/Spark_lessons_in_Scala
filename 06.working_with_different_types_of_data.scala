// Converting to Spark Types
import org.apache.spark.sql.functions._

df.select(lit(5), lit("five"), lit(5.0)).show(2)
spk.sql("SELECT 5, 'five', 5.0 FROM dfTable").show(2)



// ----------- WOTKING WITH BOOLEANS ----------- 

df.where(col("Description") === "WHITE METAL LANTERN").select("Description", "InvoiceNo").show(2) // =!=
+-------------------+---------+
|        Description|InvoiceNo|
+-------------------+---------+
|WHITE METAL LANTERN|   536365|
|WHITE METAL LANTERN|   536373|
+-------------------+---------+

df.where("InvoiceNo = 536373").show(2, false)
+---------+---------+----------------------------------+
|InvoiceNo|StockCode|Description                       |
+---------+---------+----------------------------------+
|536373   |85123A   |WHITE HANGING HEART T-LIGHT HOLDER|
|536373   |71053    |WHITE METAL LANTERN               |
+---------+---------+----------------------------------+

df.where("InvoiceNo <> 536365").show(2, false)

val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")
df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter)).show()

spk.sql("""
  SELECT * 
  FROM dfTable 
  WHERE StockCode in ("DOT") AND(UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)
  """).show(2)
+---------+---------+--------------+--------+-------------------+---------+----------+--------------+
|InvoiceNo|StockCode|   Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
+---------+---------+--------------+--------+-------------------+---------+----------+--------------+
|   536544|      DOT|DOTCOM POSTAGE|       1|2010-12-01 14:32:00|   569.77|      null|United Kingdom|
|   536592|      DOT|DOTCOM POSTAGE|       1|2010-12-01 17:06:00|   607.49|      null|United Kingdom|
+---------+---------+--------------+--------+-------------------+---------+----------+--------------+

val DOTCodeFilter = col("StockCode") === "DOT"
val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")

df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
  .where("isExpensive")
  .select("unitPrice", "isExpensive").show(5)

spk.sql("""
  SELECT UnitPrice, (StockCode = 'DOT' AND (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
  FROM dfTable
  WHERE (StockCode = 'DOT' AND (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))
""").show()
+---------+-----------+
|unitPrice|isExpensive|
+---------+-----------+
|   569.77|       true|
|   607.49|       true|
+---------+-----------+

df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
  .filter("isExpensive")
  .select("Description", "UnitPrice").show(5)
+--------------+---------+
|   Description|UnitPrice|
+--------------+---------+
|DOTCOM POSTAGE|   569.77|
|DOTCOM POSTAGE|   607.49|
+--------------+---------+



// ----------- WORKING WITH NUMBERS -----------

// numerical expression
df.selectExpr("CustomerId", "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)
spark.sql("SELECT customerId, (POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity FROM dfTable").show(2)
+----------+------------------+
|CustomerId|      realQuantity|
+----------+------------------+
|   17850.0|239.08999999999997|
|   17850.0|          418.7156|
+----------+------------------+

// rounded float
df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(3)
spark.sql("SELECT round(UnitPrice, 1) as rounded, UnitPrice FROM dfTable").show(3)
// You can round down by using the bround
+-------+---------+
|rounded|UnitPrice|
+-------+---------+
|    2.6|     2.55|
|    3.4|     3.39|
|    2.8|     2.75|
+-------+---------+

// the correlation of two columns
df.select(corr("Quantity", "UnitPrice")).show()
spark.sql("SELECT corr(Quantity, UnitPrice) FROM dfTable").show()
+-------------------------+
|corr(Quantity, UnitPrice)|
+-------------------------+
|     -0.04112314436835551|
+-------------------------+

// compute summary statistics
df.describe().show()
+-------+-----------------+------------------+
|summary|        InvoiceNo|         StockCode|
+-------+-----------------+------------------+
|  count|             3108|              3108|
|   mean| 536516.684944841|27834.304044117645|
| stddev|72.89447869788873|17407.897548583845|
|    min|           536365|             10002|
|    max|          C536548|              POST|
+-------+-----------------+------------------+

val colName = "UnitPrice"
val quantileProbs = Array(0.5)
val relError = 0.05
df.stat.approxQuantile("UnitPrice", quantileProbs, relError)

// cross-tabulation or frequent item pairs
df.stat.crosstab("StockCode", "Quantity").show(2)

df.stat.freqItems(Seq("StockCode", "Quantity")).show()
+--------------------+--------------------+
| StockCode_freqItems|  Quantity_freqItems|
+--------------------+--------------------+
|[90214E, 20728, 2...|[200, 128, 23, 32...|
+--------------------+--------------------+

// add a unique ID to each row
df.select(col("InvoiceNo"), monotonically_increasing_id()).show(5)
+---------+-----------------------------+
|InvoiceNo|monotonically_increasing_id()|
+---------+-----------------------------+
|   536365|                            0|
|   536365|                            1|
|   536365|                            2|
|   536365|                            3|
|   536365|                            4|
+---------+-----------------------------+



// ----------- WORKING WITH STRINGS -----------

// capitalize every words separated by a space
df.select(initcap(col("Description"))).show(2, false)
spark.sql("SELECT initcap(Description) FROM dfTable").show(2)
+----------------------------------+
|initcap(Description)              |
+----------------------------------+
|White Hanging Heart T-light Holder|
|White Metal Lantern               |
+----------------------------------+

// cast strings in uppercase and lowercase
df.select(col("Description"), lower(col("Description")), upper(lower(col("Description")))).show(2)
spark.sql("SELECT Description, lower(Description), Upper(lower(Description)) FROM dfTable").show(2)
+--------------------+--------------------+-------------------------+
|         Description|  lower(Description)|upper(lower(Description))|
+--------------------+--------------------+-------------------------+
|WHITE HANGING HEA...|white hanging hea...|     WHITE HANGING HEA...|
| WHITE METAL LANTERN| white metal lantern|      WHITE METAL LANTERN|
+--------------------+--------------------+-------------------------+

// adding or removing spaces around a string
df.select(
  ltrim(lit(" HELLO ")).as("ltrim"),
  rtrim(lit(" HELLO ")).as("rtrim"),
  trim(lit(" HELLO ")).as("trim"),
  lpad(lit("HELLO"), 3, " ").as("lp"),
  rpad(lit("HELLO"), 10, " ").as("rp")).show(2)

spark.sql("""
  SELECT
  ltrim(' HELLLOOOO '),
  rtrim(' HELLLOOOO '),
  trim(' HELLLOOOO '),
  lpad('HELLOOOO ', 3, ' '),
  rpad('HELLOOOO ', 10, ' ')
  FROM dfTable
""").show(2)
+------+------+-----+---+----------+
| ltrim| rtrim| trim| lp|        rp|
+------+------+-----+---+----------+
|HELLO | HELLO|HELLO|HEL|HELLO     |
|HELLO | HELLO|HELLO|HEL|HELLO     |
+------+------+-----+---+----------+

// RegEx
val simpleColors = Seq("black", "white", "red", "green", "blue")
val regexString = simpleColors.map(_.toUpperCase).mkString("|")
// the | signifies `OR` in regular expression syntax
df.select(
  regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"), col("Description"))
  .show(2)

spark.sql("""
  SELECT regexp_replace(Description, 'BLACK|WHITE|RED|GREEN|BLUE', 'COLOR') as color_clean, Description
  FROM dfTable
""").show(2)
+--------------------+--------------------+
|         color_clean|         Description|
+--------------------+--------------------+
|COLOR HANGING HEA...|WHITE HANGING HEA...|
| COLOR METAL LANTERN| WHITE METAL LANTERN|
+--------------------+--------------------+

// replace given characters with other
df.select(translate(col("Description"), "LEET", "1337"), col("Description"))
  .show(2)
spark.sql("SELECT translate(Description, 'LEET', '1337'), Description FROM dfTable")
  .show(2)
+----------------------------------+--------------------+
|translate(Description, LEET, 1337)|         Description|
+----------------------------------+--------------------+
|              WHI73 HANGING H3A...|WHITE HANGING HEA...|
|               WHI73 M37A1 1AN73RN| WHITE METAL LANTERN|
+----------------------------------+--------------------+

val regexString = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
// the | signifies OR in regular expression syntax
df.select(
  regexp_extract(col("Description"), 
  regexString, 1).alias("color_clean"),
  col("Description")).show(2)

spark.sql("""
  SELECT regexp_extract(Description, '(BLACK|WHITE|RED|GREEN|BLUE)', 1),
  Description
  FROM dfTable
""").show(2)
+-----------+--------------------+
|color_clean|         Description|
+-----------+--------------------+
|      WHITE|WHITE HANGING HEA...|
|      WHITE| WHITE METAL LANTERN|
+-----------+--------------------+

// check for their existence
val containsBlack = col("Description").contains("BLACK")
val containsWhite = col("DESCRIPTION").contains("WHITE")
df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
  .where("hasSimpleColor")
  .select("Description").show(3, false)

spark.sql("""
  SELECT Description FROM dfTable
  WHERE instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1
""").show(3)
+----------------------------------+
|Description                       |
+----------------------------------+
|WHITE HANGING HEART T-LIGHT HOLDER|
|WHITE METAL LANTERN               |
|RED WOOLLY HOTTIE WHITE HEART.    |
+----------------------------------+

val simpleColors = Seq("black", "white", "red", "green", "blue")
val selectedColumns = simpleColors.map(color => {
    col("Description").contains(color.toUpperCase).alias(s"is_$color")
    }):+expr("*") // could also append this value

df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
.select("Description").show(3, false)
+----------------------------------+
|Description                       |
+----------------------------------+
|WHITE HANGING HEART T-LIGHT HOLDER|
|WHITE METAL LANTERN               |
|RED WOOLLY HOTTIE WHITE HEART.    |
+----------------------------------+



// ----------- WORKING WITH DATES TIMESTAMPS -----------

// set a session local timezone : spark.conf.sessionLocalTimeZone in the SQL config
// accordingly to the Java TimeZone format.

val dateDF = spark.range(10)
  .withColumn("today", current_date())
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")
dateDF.show(3)
+---+----------+--------------------+
| id|     today|                 now|
+---+----------+--------------------+
|  0|2020-03-03|2020-03-03 19:56:...|
|  1|2020-03-03|2020-03-03 19:56:...|
|  2|2020-03-03|2020-03-03 19:56:...|
+---+----------+--------------------+

//let’s add and subtract five days from today
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)
spark.sql("SELECT date_sub(today, 5), date_add(today, 5) FROM dateTable").show(1)
+------------------+------------------+
|date_sub(today, 5)|date_add(today, 5)|
+------------------+------------------+
|        2020-02-27|        2020-03-08|
+------------------+------------------+

dateDF.withColumn("week_ago", date_sub(col("today"), 7))
    .select(datediff(col("week_ago"), col("today"))).show(1)
+-------------------------+
|datediff(week_ago, today)|
+-------------------------+
|                       -7|
+-------------------------+

dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))
    .select(months_between(col("start"), col("end"))).show(1)

spark.sql("""
    SELECT to_date('2016-01-01'), months_between('2016-01-01', '2017-01-01'), datediff('2016-01-01', '2017-01-01')
    FROM dateTable
""").show(1)
+--------------------------------+
|months_between(start, end, true)|
+--------------------------------+
|                    -16.67741935|
+--------------------------------+

val dateFormat = "yyyy-dd-MM"
val cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))

cleanDateDF.show(1)
cleanDateDF.createOrReplaceTempView("dateTable2")

spark.sql("SELECT to_date(date, 'yyyy-dd-MM'), to_date(date2, 'yyyy-dd-MM'), to_date(date) FROM dateTable2").show()
+----------+----------+
|      date|     date2|
+----------+----------+
|2017-11-12|2017-12-20|
+----------+----------+

cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
spark.sql("SELECT to_timestamp(date, 'yyyy-dd-MM'), to_timestamp(date2, 'yyyy-dd-MM') FROM dateTable2").show()
+----------------------------------+
|to_timestamp(`date`, 'yyyy-dd-MM')|
+----------------------------------+
|               2017-11-12 00:00:00|
+----------------------------------+

// casting
spark.sql("SELECT cast(to_date('2017-01-01', 'yyyy-dd-MM') as timestamp)").show()
+------------------------------------------------------+
|CAST(to_date('2017-01-01', 'yyyy-dd-MM') AS TIMESTAMP)|
+------------------------------------------------------+
|                                   2017-01-01 00:00:00|
+------------------------------------------------------+

// filtering
cleanDateDF.filter(col("date2") > "'2017-12-12'").show()
+----------+----------+
|      date|     date2|
+----------+----------+
|2017-11-12|2017-12-20|
+----------+----------+



// ----------- WORKING WITH NULLS IN DATA -----------

// Coalesce - function to allow you to select the first non-null value from a set of columns
df.select(coalesce(col("Description"), col("CustomerId"))).show()
+---------------------------------+
|coalesce(Description, CustomerId)|
+---------------------------------+
|             WHITE HANGING HEA...|
|              WHITE METAL LANTERN|

df.na.drop("any")
df.na.drop("all") // only if all values are null or NaN
// SELECT * FROM dfTable WHERE Description IS NOT NULL

// apply this to certain sets of columns
df.na.drop("all", Seq("StockCode", "InvoiceNo"))

// fill all null values in columns of type String
df.na.fill("All Null values become this string")

//of type Integer 
df.na.fill(5, Seq("StockCode", "InvoiceNo"))

df.na.replace("Description", Map("" -> "UNKNOWN"))



// ----------- WORKING WITH COMPLEX TYPES -----------

// Structs (think of structs as df within df)
val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexTable")
complexDF.show(2)
+--------------------+
|             complex|
+--------------------+
|[WHITE HANGING HE...|
|[WHITE METAL LANT...|
+--------------------+

complexDF.select("complex.Description").show(2)
spark.sql("SELECT complex.* FROM complexTable").show(2)
+--------------------+---------+
|         Description|InvoiceNo|
+--------------------+---------+
|WHITE HANGING HEA...|   536365|
| WHITE METAL LANTERN|   536365|
+--------------------+---------+

// Arrays
df.select(split(col("Description"), " ")).show(2)
spark.sql("SELECT split(Description, ' ') FROM dfTable").show(2)
+---------------------+
|split(Description,  )|
+---------------------+
| [WHITE, HANGING, ...|
| [WHITE, METAL, LA...|
+---------------------+

df.select(split(col("Description"), " ").alias("array_col")).selectExpr("array_col[0]").show(2)
spark.sql("SELECT split(Description, ' ')[0] FROM dfTable").show(2)
+------------+
|array_col[0]|
+------------+
|       WHITE|
|       WHITE|
+------------+

df.select(size(split(col("Description"), " "))).show(2)
+---------------------------+
|size(split(Description,  ))|
+---------------------------+
|                          5|
|                          3|
+---------------------------+

// whether this array contains a value:
df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)
spark.sql("SELECT array_contains(split(Description, ' '), 'WHITE') FROM dfTable").show(2)
+--------------------------------------------+
|array_contains(split(Description,  ), WHITE)|
+--------------------------------------------+
|                                        true|
|                                        true|
+--------------------------------------------+

df.withColumn("splitted", split(col("Description"), " "))
    .withColumn("exploded", explode(col("splitted")))
    .select("Description", "InvoiceNo", "exploded").show(2)
+--------------------+---------+--------+
|         Description|InvoiceNo|exploded|
+--------------------+---------+--------+
|WHITE HANGING HEA...|   536365|   WHITE|
|WHITE HANGING HEA...|   536365| HANGING|
+--------------------+---------+--------+

// Maps
// the map function and key-value pairs of columns. You then can select
// them just like you might select from an array:

df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2)
// SELECT map(Description, InvoiceNo) as complex_map FROM dfTable WHERE Description IS NOT NULL
+--------------------+
|         complex_map|
+--------------------+
|[WHITE HANGING HE...|
|[WHITE METAL LANT...|
+--------------------+

df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
  .selectExpr("complex_map['WHITE METAL LANTERN']").show(2)
+--------------------------------+
|complex_map[WHITE METAL LANTERN]|
+--------------------------------+
|                            null|
|                          536365|
+--------------------------------+

df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
  .selectExpr("explode(complex_map)").show(2)
+--------------------+------+
|                 key| value|
+--------------------+------+
|WHITE HANGING HEA...|536365|
| WHITE METAL LANTERN|536365|
+--------------------+------+



// ----------- WORKING WITH JSON -----------

val jsonDF = spark.range(1).selectExpr("""
  '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")
jsonDF.show()
+--------------------+
|          jsonString|
+--------------------+
|{"myJSONKey" : {"...|
+--------------------+

jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
    json_tuple(col("jsonString"), "myJSONKey")).show(2)
+------+--------------------+
|column|                  c0|
+------+--------------------+
|     2|{"myJSONValue":[1...|
+------+--------------------+

// turn a StructType into a JSON string
df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct"))).show()
+-----------------------+
|structstojson(myStruct)|
+-----------------------+
|   {"InvoiceNo":"536...|
|   {"InvoiceNo":"536...|

// This func also accepts a dictionary (map) of parameters that are
// the same as the JSON data source (to parse, this requires you to specify a schema
import org.apache.spark.sql.types._

val parseSchema = new StructType(Array(
    StructField("InvoiceNo", StringType, true),
    StructField("Description", StringType, true)))
df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")).alias("newJSON"))
    .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2)
+----------------------+--------------------+
|jsontostructs(newJSON)|             newJSON|
+----------------------+--------------------+
|  [536365, WHITE HA...|{"InvoiceNo":"536...|
|  [536365, WHITE ME...|{"InvoiceNo":"536...|
+----------------------+--------------------+



// ----------- USER DEFINED FUNCTIONS -----------

// custom transformations using Scala and even use external libraries.
val udfExampleDF = spark.range(5).toDF("num")
def power3(number:Double):Double = number * number * number 
power3(2.0)
res74: Double = 8.0

val power3udf = udf(power3(_:Double):Double)
udfExampleDF.select(power3udf(col("num"))).show()
+--------+
|UDF(num)|
+--------+
|     0.0|
|     1.0|
|     8.0|
|    27.0|

// register this UDF as a Spark SQL function 
// valuable because it makes it simple to use this function
spark.udf.register("power3", power3(_:Double):Double)
udfExampleDF.selectExpr("power3(num)").show(2)
+-------------------------------+
|UDF:power3(cast(num as double))|
+-------------------------------+
|                            0.0|
|                            1.0|
+-------------------------------+