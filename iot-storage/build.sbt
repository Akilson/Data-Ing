name := "iot-storage"

scalaVersion := "2.13.12"
val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "3.5.1",
  "io.circe" %% "circe-core" % "0.14.7",
  "io.circe" %% "circe-parser" % "0.14.7",
  "org.typelevel" %% "cats-effect" % "3.5.4",
   // Spark for data processing
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1",

  // AWS S3/Minio integration
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.261",

  // JSON processing
  "com.typesafe.play" %% "play-json" % "2.9.4",
    // Configuration
  "com.typesafe" % "config" % "1.4.3",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.hadoop" % "hadoop-client-api" % "3.3.4"
)
