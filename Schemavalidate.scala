val csv = sc.textFile("bank_details_schema.csv")
val hms = csv.map(line => line.split(",").map(_.trim))
val hmslist = hms.map(_.toSeq.toList)

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._

val result = for (a <- hmslist) yield StructField(a(0),CatalystSqlParser.parseDataType(a(1)),a(2).toBoolean)
val customschema = StructType(result.collect)
//INFER SCHEMA OF TEST FILE
val sqlContext = new org.apache.spark.sql.SQLContext(sc);  
val df_right = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    //.option("inferSchema", "true") // Automatically infer data types
    .option("dateFormat", "yyyymmdd")
    .load("Bank_Sale.csv")
val df_wrong = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("Bank_Sale_wrong.csv")
