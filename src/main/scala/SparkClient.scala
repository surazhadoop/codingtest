import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark.sql.SparkSession

object SparkClient {

  def getSparkContext()={
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkAndHive")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse2")
      .enableHiveSupport()
      .getOrCreate()

    val perm = DefaultAWSCredentialsProviderChain.getInstance()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", perm.getCredentials.getAWSAccessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", perm.getCredentials.getAWSSecretKey)
    spark
  }
}
