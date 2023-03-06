name := "CodingTest"

version := "0.1"

scalaVersion := "2.13.8"
val sparkVersion = "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % sparkVersion
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.12.411"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.12.411"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.3.0-SNAP3"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "3.3.1_1.4.0"
libraryDependencies += "com.opencsv" % "opencsv" % "5.5.2"


