import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3ClientBuilder}

object AWSClient {
   var awsCredentialPath=""

  def getS3Client()={
    val creds = new ProfileCredentialsProvider(awsCredentialPath, "default")
    val s3client = AmazonS3ClientBuilder
      .standard().withCredentials(creds)
      //.withRegion(region)
      .build();
    s3client
  }

}
