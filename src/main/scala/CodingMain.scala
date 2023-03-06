import AWSClient.getS3Client
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.opencsv.{CSVWriter, ICSVWriter}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.{File, StringWriter}
import java.util.UUID

object CodingMain {

  /*
  This method would work for local file system.
   */
  def getRDDWithoutHeaderLocal(sc:SparkContext,path: String) = {

    val directoryPath = new File(path)
    //List of all files and directories
    val files = directoryPath.listFiles()
    val filesRDD=files.map(fileName=>{
      println("fileName:"+fileName)
      val fileRDD=sc.textFile(fileName.getPath)
      fileRDD.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
    })
    val finalRDD=filesRDD.reduce((rdd1,rdd2)=>{rdd1 union rdd2})
    finalRDD
  }

  /*
  This method would read all the files from a given bucket of AWS
  It creates rdd of each file,
  removes the header out of it and then club all the rdd together

   */
  def getRDDWithoutHeaderAWS(sc: SparkContext, inputPath: String) = {

    val s3client=getS3Client
    //s3://assignment04mar2023/input/

    val fileNameArr=inputPath.split("//")
    val bucketAndPrefix=fileNameArr(1).split("/")
    val bucketName=bucketAndPrefix(0)
    val prefix=fileNameArr(1).substring(bucketName.length+1)

    val request = new ListObjectsRequest()
      .withBucketName(bucketName)
      .withPrefix(prefix);

    val objectListing = s3client.listObjects(request)

    import scala.jdk.CollectionConverters._
    val filesRDD=objectListing.getObjectSummaries().asScala.flatMap(fileName=>{
      println(fileName)
      val path=s"s3a://${fileName.getBucketName}/${fileName.getKey}"
      if(!fileName.getKey().endsWith("/")) {
        val fileRDD = sc.textFile(path)
        Some(fileRDD.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it))
      }else None
    })

    val finalRDD = filesRDD.reduce((rdd1, rdd2) => {
      rdd1 union rdd2
    })
    finalRDD
  }

  def main(args: Array[String]): Unit = {

    if(args.length<3){
      throw new RuntimeException("Invalid Number of Arguments.Please pass 3 Arguments\n" +
        "1. inputPath \n" +
        "2. outputPath\n" +
        "3. Fully Qualified awsCredential Path")
    }

    val inputPath=args(0)
    val outputPath=args(1)+UUID.randomUUID()
    val awsCredential=args(2)
    val mode="aws"// can be local too

    //Example Path
    /*val inputPath="s3://assignment04mar2023/input/"
    val outputPath=s"s3a://assignment04mar2023/output_${UUID.randomUUID()}/"
    val awsCredential="/home/hduser/.aws/credentials"
*/
    AWSClient.awsCredentialPath=awsCredential

    val sc=SparkClient.getSparkContext()
    val output=process(sc.sparkContext,mode,inputPath)
    val resultRDD=output.map(row=>row._1+"\t"+row._2)
    resultRDD.coalesce(1).saveAsTextFile(outputPath)
  }

  //Using mode,to run the program in local mode too.
   def process(sc: SparkContext,mode:String,path:String) = {
    val rddHeaderRemoved = {
      mode match {
        case "aws" => getRDDWithoutHeaderAWS (sc, path)
        case _=> getRDDWithoutHeaderLocal(sc,path)
      }
    }
    val newKeyValue = getKeyValue(rddHeaderRemoved)

    /*The above key,value would be aggregated to
      1_2,1
      1_3,2
      2_4,3
      */
    val keyValueCount = newKeyValue.reduceByKey(_ + _)

    //Get me only those key,value which occured odd number of time
    val oddNumberValues = keyValueCount.filter(_._2 % 2 == 1)
    /*
    1_2, 1
    2_4, 3
    */
    //oddNumberValues.collect.foreach(println)


    /* We need to handle such cases too.
    1 -> 2
    1 -> 3
    1 -> 4

    They will also form
    (1_2,1)
    (1_3,1)
    (1_4,1)

    We will collect key,part and count it. This will become
    1,3, which means it is repeated 3 times with unique value.
    We will have to ignore it.
    */
    val uniqueKeyWithNoOccurance = oddNumberValues.map(row => (row._1.split("_")(0), 1)).reduceByKey(_ + _).filter(_._2 == 1).map(_._1).collect()
    val filterUniqueKeyWithNoOccurance = oddNumberValues.filter(row => uniqueKeyWithNoOccurance.contains(row._1.split("_")(0)))
    filterUniqueKeyWithNoOccurance.map(data=>{
      val kv=data._1.split("_")
      (kv(0),kv(1))
    })
  }

  /*
    Given the RDD of rows,
    For each row:
     it will remove all empty row.
    It will replace all " by empty space
    It will replace all \t by ,

    For every key value, Lets create key_value,1
    That way we will understand the count of every key,value
    1,2 => 1_2,1
    1,3 => 1_3,1
    1	3 => 1_3,1
    2,4 => 2_4,1
    2	4 => 2_4,1
    2,4 => 2_4,1
     */
  private def getKeyValue(rddHeaderRemoved: RDD[String]) = {
    val newKeyValue = rddHeaderRemoved.flatMap(row => {

      //If row is empty return None
      if (StringUtils.isBlank(row)) None
      else {
        //replace all tabs by ,
        val replaceQuotes = row.replace("\"", "")
        val standardRow = replaceQuotes.replaceAll("\t", ",")
        val cols = standardRow.split(",");

        var key = cols(0)
        var value = cols(1)
        //If either key or value is not-numeric, lets ignore processing it
        if (!StringUtils.isNumeric(key) || !StringUtils.isNumeric(value)) None
        else {
          //if key or value is empty, use 0
          if (StringUtils.isBlank(key)) key = "0"
          if (StringUtils.isBlank(value)) value = "0"
          val newKey = key + "_" + value
          Some(newKey, 1)
        }
      }
    })
    newKeyValue
  }
}
