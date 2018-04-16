package com.prime.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.sql.SQLException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.types.{ DataType, StructType }

/**
 * ### Object Name:WriteToHive
 * ### Description: This objects contains below methods of load/update or overwrite Hive Table
 * ### 							a) 	loadHiveTable - To append data into an existing Hive Table
 * ###							b)  overwriteHiveTable - Rewrite existing partitioned Hive table as per the given specifications
 * ###							C)  overwriteHivePartition - Overwrite existing partitioned Hive table
 * ### Code Version: 0.01
 * ### Change Log: <Date:08-03-2018>
 */

case class HiveTableNotFoundException(msg: String) extends Exception
case class SchemaMismatchException(msg: String) extends Exception

object WriteToHive {

  /**
   * Append data into existing Hive Table
   * for the given field names
   * @param hiveWritableDF - input DataFrame
   * @param db - Target Database
   * @param tableName - Target table Name
   */

  def loadHiveTable(hiveWritableDF: DataFrame, db: String, tableName: String) {

    val tableIdentifier = db.trim() + "." + tableName.trim()
    val spark = hiveWritableDF.sparkSession
    val catalog = spark.catalog

    if (!catalog.tableExists(db, tableName)) {
      throw new HiveTableNotFoundException(LocalDateTime.now() + " : FATAL : Hive Table " + tableIdentifier + " doesn't exist")
    }

    val dfSchema = hiveWritableDF.schema
    val hiveTableSchema = spark.read.table(tableIdentifier).schema

    validateSchema(dfSchema, hiveTableSchema)

    hiveWritableDF.write.mode("append").insertInto(tableIdentifier)

  }

  /**
   * create a New table after overwriting existing data
   * @param hiveWritableDF - input DataFrame
   * @param db - Target Database
   * @param tableName - Target table Name
   * @param partitionBy - Specify partition if table is to required to be partitioned
   * @param format -Data source format of target table
   */

  def overwriteHiveTable(hiveWritableDF: DataFrame, db: String, tableName: String, partitionBy: String, format: String) {

    val tableIdentifier = db.trim() + "." + tableName.trim()
    val spark = hiveWritableDF.sparkSession
    hiveWritableDF.write.mode("overwrite").format(format).partitionBy(partitionBy).saveAsTable(tableIdentifier)

  }

  def overwriteHiveTable(hiveWritableDF: DataFrame, db: String, tableName: String, partitionBy: String) {

    overwriteHiveTable(hiveWritableDF, db, tableName, partitionBy, "parquet")

  }

  def overwriteHiveTable(hiveWritableDF: DataFrame, db: String, tableName: String) {
    val tableIdentifier = db.trim() + "." + tableName.trim()
    val spark = hiveWritableDF.sparkSession
    val format = "text"

    hiveWritableDF.write.mode("overwrite").format("parquet").saveAsTable(tableIdentifier)

  }

  /**
   * Load an existing Hive table using merge approach
   * to overwrite matching partitions
   * @param hiveWritableDF - input DataFrame
   * @param db - Target Database
   * @param tableName - Target table Name
   */

  def overwriteHivePartition(hiveWritableDF: DataFrame, db: String, tableName: String) {

    val tableIdentifier = db.trim() + "." + tableName.trim()
    val spark = hiveWritableDF.sparkSession
    val catalog = spark.catalog

    if (!catalog.tableExists(db, tableName)) {
      throw new HiveTableNotFoundException(LocalDateTime.now() + " : FATAL : Hive Table " + tableIdentifier + " doesn't exist")
    }

    val dfSchema = hiveWritableDF.schema
    val hiveTableSchema = spark.read.table(tableIdentifier).schema

    validateSchema(dfSchema, hiveTableSchema)

    val dfHiveMetadata = spark.sql("DESCRIBE FORMATTED " + tableIdentifier)

    if (!isHiveTablePartitioned(dfHiveMetadata)) {
      throw new SchemaMismatchException(LocalDateTime.now() + " : FATAL : Hive Table " + tableIdentifier + " is not the partitioned table")
    }

    spark.conf.set("hive.exec.dynamic.partition", true)

    spark.conf.set("hive.exec.dynamic.partition.moden", "nonstrict")

    hiveWritableDF.write.mode("overwrite").insertInto(tableIdentifier)

  }

  /**
   * Validates schema of input Dataframe and
   * Target Hive Table
   * @param dfSchema - input DataFrame Schema
   * @param hiveTableSchema - Hive Table Schema
   * @param tableName - Target table Name
   */

  def validateSchema(dfSchema: StructType, hiveTableSchema: StructType) {

    val dfDataType = dfSchema.map(_.dataType.toString())
    val hiveTableDataType = hiveTableSchema.map(_.dataType.toString())

    if (dfDataType.size != hiveTableDataType.size) {
      throw new SchemaMismatchException(LocalDateTime.now() + " : FATAL : Number of Columns in Input Dataframe doesnt match with Hive Table ")
    }

    if ((dfDataType zip hiveTableDataType).filter(x => (x._1 != "StringType" && x._2 != "StringType" && x._1 != x._2)).size > 0) {

      throw new SchemaMismatchException(LocalDateTime.now() + " : FATAL : Input Dataframe Schema doesnt match with Hive Table Schema ")
    }

  }

  /**
   * Validates if Hive Table is partitioned not not
   * @param metadata - Metadata Catalog of Hive Table
   */

  def isHiveTablePartitioned(metadata: DataFrame): Boolean = {

    val propList = metadata.select("col_name").collect().map(_(0)).toList

    if (propList.slice(propList.indexOf("# Partition Information"), propList.indexOf("# Detailed Table Information")).filter(_ != "# Partition Information").filter(_ != "# col_name").size > 0) {
      return true

    } else {
      return false
    }
  }
  
  /*
   def main(args: Array[String]) {
    val spark = SparkSession
			.builder()
			.appName("Spark Hive Example")
			//.config("hive.metastore.uris", "jdbc:hive2://lxlcmn002d.primetherapeutics.com:10000")
			.config("hive.exec.dynamic.partition", "true")
			.config("hive.exec.dynamic.partition.mode", "nonstrict")
			.enableHiveSupport()
			.getOrCreate();

    val hiveWritableDF = spark.read.
    format("com.databricks.spark.csv").
    option("delimiter","|").
    option("header","true").
    load("/tmp/edhuploadsbx/HMS_Individual_Profiles.tab");
	//hiveWritableDF.show()
	var db = "edhuploadsbx"
	var tableName = "T_HMS_INDV_PRFL"
	loadHiveTable(hiveWritableDF,db,tableName)
    
     
  }
  * 
  */

     
 }
 
