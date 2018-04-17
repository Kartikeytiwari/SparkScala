import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import java.util.List
import scala.collection.mutable.ArrayBuffer

case class NoSchemaMatch(msg:String) extends Exception //CustomException in case of No records match the desired schema

object formatValidation {
   
  implicit val codec= Codec("UTF-8")
codec.onMalformedInput(CodingErrorAction.REPLACE)
codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    //Source.fromFile("").codec.decoder.decode()
   /**
   * Create custom StructType based on schema file
   * @param schemaFilePath: path of schema file
   */
  def createSchema(schemaFilePath: String): StructType = {
    val sc=SparkContext.getOrCreate()
    val csvSchema = sc.textFile(schemaFilePath)
    val field = csvSchema.map(line => line.split(",").map(_.trim))
    val fieldList = field.map(_.toSeq.toList)
    val result = for (a <- fieldList) yield StructField(a(0),CatalystSqlParser.parseDataType(a(1)),a(2).toBoolean)
    val desiredSchema = StructType(result.collect)
    return desiredSchema
  }
   /**
   * create output DataFrame based on the records matching the defined schema, neglect non-matching records and store in another DataFrame
   * @param schemaFileName: name of schema file  
   * @param schemaFilePath: path of schema file
   * @param spark: spark session
   */
  
  def checkSchema(schemaFileName:String,schemaFilePath:String,spark: SparkSession,filePath:String,delimiter:String):ArrayBuffer[DataFrame] = {
    
    val outputArray = ArrayBuffer[DataFrame]()
    var schemaFullFilePath = schemaFilePath + "/" + schemaFileName
    var validDf:DataFrame=null 
    var mismatchedDf:DataFrame=null
    val desiredSchema = createSchema(schemaFullFilePath)
    println(desiredSchema)
    val  corruptSchema = desiredSchema.add("_corrupt_record","string")
    println(corruptSchema)
    try {
      println("inside try")
      val inputDf = spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter",delimiter)
        .schema(corruptSchema)
        .option("mode", "PERMISSIVE")
        .option("ColumnNameOfCorruptRecord","_corrupt_record")
        .load(filePath) //DataFrame contains all Fields without schema conformance 
      inputDf.show
      validDf = inputDf.filter("_corrupt_record is null")
      println(validDf.count)
     
      mismatchedDf = inputDf.filter(inputDf("_corrupt_record") !== null)
      println(mismatchedDf.count)
        

  		if (validDf.count == 0) throw new NoSchemaMatch("all records are discarded")
      
      outputArray += validDf
      outputArray += mismatchedDf
      
      outputArray
    }catch{
               case ex:NoSchemaMatch => {
    			        println("all records are discarded")
    		      }
               
          		case ex: NullPointerException => {
                      println("NullPointerException")
          		 }
          		 
       		  case ex: Exception => {
                    println(ex.getStackTrace)
                }
               
		  }
    
    return outputArray
   
  }
}
object formathelperClass {
  var schemaFileName ="xyz.txt"
  var schemaFilePath = "/tmp/sbx"
  var filePath= "/directory"
  
  val spark = SparkSession
			.builder()
			.appName("Spark Hive Example")
			//.config("hive.metastore.uris", "jdbc:hive2://lxlcmn002d.primetherapeutics.com:10000")
			.config("hive.exec.dynamic.partition", "true")
			.config("hive.exec.dynamic.partition.mode", "nonstrict")
			.enableHiveSupport()
			.getOrCreate();
  
  def main(args: Array[String]){

    var outputArray = ArrayBuffer[DataFrame]()
     
    outputArray = formatValidation.checkSchema(schemaFileName, schemaFilePath, spark, filePath,"\t")
    println("Function returned "+outputArray(0).count)
    println("=====================================================")
    println(outputArray(0).schema)
    var db = "exampleDB"
  	var tableName = "T_SCHEMA_VALIDATE"
  	WriteToHive.loadHiveTable(outputArray(0),db,tableName)
  }
}
