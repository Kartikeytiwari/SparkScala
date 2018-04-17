import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import java.io.FileNotFoundException
import java.io.IOException
import java.io.Serializable

case class LengthMismatchException(msg: String) extends Exception
//object CustomUtility extends Serializable{  
//   val parseFixedLengths:(String,Seq[Int])=> Seq[String] = (line, lengths) => {
//   
//    lengths.indices.foldLeft((line, Array.empty[String])) {
//      case ((rem, fields), idx) =>
//        val len = lengths(idx)
//        val fld = rem.take(len)
//        (rem.drop(len), fields :+ fld)
//    }._2}
//}

object readFromHDFS extends Serializable{
  
  //A typeMap that converts Strings to their respective DataType
  val types = classOf[DataTypes]
    .getDeclaredFields()
    .filter(f => java.lang.reflect.Modifier.isStatic(f.getModifiers()))
    .map(f => f.get(new DataTypes()).asInstanceOf[DataType])
  val typeMap = types.map(t => (t.getClass.getSimpleName.replace("$", ""), t)).toMap
    
  /**
   * parse a string based on the given field size.
   * @param line: string input
   * @param lengths: sequence containing field size in fixed length input
   *
 	 */
val parseFixedLengths:(String,Seq[Int])=> Seq[String] = (line, lengths) => {
   
    lengths.indices.foldLeft((line, Array.empty[String])) {
      case ((rem, fields), idx) =>
        val len = lengths(idx)
        val fld = rem.take(len)
        (rem.drop(len), fields :+ fld)
    }._2}

  /**
   * creating a DF for a fixed width input file
   * @param rawFilePath: path of file in HDFS
   * @param lengths: sequence containing field size in fixed length input
   * @param spark: spark session
   * @param columnNames : names of the columns of dataframe
   */
  def createDFfromFixedWidth(rawFilePath: String, lengths: Seq[Int], spark: SparkSession,desiredSchema: StructType,isHeader: Boolean): DataFrame = {
   
    val colNames = desiredSchema.map(f => s"${f.name}")
    val colDatTypes = desiredSchema.map(f => s"${f.dataType}")
    var outputDf: DataFrame = null //outputDF
    var stringDf: DataFrame = null // tempDF consisting of String Type Columns
    import spark.implicits._
    
    try {
        val lines = spark.read.textFile(rawFilePath).filter(!_.isEmpty)
        //check For Field Lengths in FixedWidth rows
        for (i <- lines) { if (lengths.sum != i.length) throw new LengthMismatchException("length of the fields in the desired file is not equal to the given sum of lengths") }
       
        val fields = lines.map(parseFixedLengths(_, lengths)).withColumnRenamed("value", "fields")
        stringDf = lengths.indices.foldLeft(fields) {
          case (result, idx) =>
            result.withColumn(s"col_$idx", $"fields".getItem(idx))
        }.drop("fields").toDF(colNames: _*)
        //removing first Row if header present present
        if(isHeader){
        val header = stringDf.first()
        stringDf.filter(row => row != header)}
        //Imposing custom schema on StringDf as all cols are of String Type
        val cols = for (colnumber <- colNames.indices) yield stringDf(colNames(colnumber)).cast(typeMap(colDatTypes(colnumber)))
        outputDf = stringDf.select(cols:_*)

        return outputDf
        
    } catch { 
        case ex: LengthMismatchException => {
              println("Given File path is invalid / file not found ")
           }
        
        outputDf = null
        return outputDf
    }
  }
  


  /**
   * creating a DF for a input delimited file
   * @param rawFilePath: path of file in HDFS
   * @param delimiter: delimiter character through which data is separated in input file
   * @param spark: spark session
   * @param isHeader : header is there in input file or not
   * @param fileFormat : format of the input file
   */
  def createDFFromDelimetedFile(rawFilePath: String, delimiter: String, spark: SparkSession, isHeader: Boolean, fileFormat: String): DataFrame = {
    var outputDf = spark.read.format(fileFormat). // Use "csv" regardless of TSV or CSV.
      option("header", isHeader.toString). // Does the file have a header line?
      option("delimiter", delimiter). // Set delimiter to tab or comma.
      load(rawFilePath)
    return outputDf
  }

  def createDFfromDelimetedFileWithSchema(rawFilePath:String,delimiter:String,spark:SparkSession,isHeader:Boolean,fileFormat:String,customSchema:StructType):DataFrame={
    var output_df = spark.read.format(fileFormat).     // Use "csv" regardless of TSV or CSV.
                  option("header",isHeader.toString ).  // Does the file have a header line?
                  option("delimiter", delimiter). // Set delimiter to tab or comma.
                  schema(customSchema).
                  load(rawFilePath)
    return output_df
  }


  /**
   * returning a DF using custom schema
   * @param rawFileName: name of file in HDFS
   * @param rawFilePath: path of file in HDFS
   * @param delimiter: delimiter character through which data is separated in input file
   * @param spark: spark session
   * @param fileFormat : format of the input file
   * @param isCompressed : whether file is compressed or not
   * @param compressionType : type of compression used
   * @param isHeader : whether header is there in input file or not with default value false
   * @param isFooter : whether footer is there in input file or not with default value false
   * @param customSchema : customized schema
   */
  def getHadoopDataFrame(rawFileName: String, rawFilePath: String, delimiter: String, spark: SparkSession, fileFormat: String, isCompressed: Boolean, compressionType: String, isHeader: Boolean = false, isFooter: Boolean = false, desiredSchema: StructType = null, lengths: Seq[Int] = null): DataFrame = {
    var outputDf: DataFrame = null
    var fullFilePath = rawFilePath + "/" + rawFileName
    try {
      if (fileFormat.toLowerCase == "fixedlength") {
        outputDf = createDFfromFixedWidth(fullFilePath, lengths, spark, desiredSchema,isHeader)
   
      return outputDf
      } else {
        if(desiredSchema != null) {outputDf = createDFfromDelimetedFileWithSchema(fullFilePath, delimiter, spark, isHeader, "csv",desiredSchema)}
        else {outputDf = createDFFromDelimetedFile(fullFilePath, delimiter, spark, isHeader, "com.databricks.spark.csv")}
      }
     return outputDf
    } 
    catch { 
      
         case ex: FileNotFoundException =>{
            println("Given File path is invalid / file not found ")
         }
         
         case ex: IOException => {
            println("IO Exception")
         }    
         
         case ex: Exception => {
            println(ex.printStackTrace())
         }    
    } 
    return outputDf
  }
  }

object implicitHelperClass {
   var rawFileName ="xyz.tab"
  var rawFilePath = "/tmp/samplesbx"
 def main(args: Array[String]){
     val spark = SparkSession
			.builder()
			.appName("Spark Hive Example")
			//.config("hive.metastore.uris", "jdbc:hive2")
			.config("hive.exec.dynamic.partition", "true")
			.config("hive.exec.dynamic.partition.mode", "nonstrict")
			.enableHiveSupport()
			.getOrCreate();
			
    
    
    val df = readFromHDFS.getHadoopDataFrame(rawFileName, rawFilePath, "|", spark, "delimited", false, ".gz", true, false)
   
  }
  
}
