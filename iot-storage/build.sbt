name := "drone-storage"

scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  // Kafka client
  "org.apache.kafka" % "kafka-clients" % "3.6.1",

  // AWS S3 SDK v2 (compatible with MinIO)
  "software.amazon.awssdk" % "s3" % "2.25.27",

  // Optional: JSON support for parsing drone messages
  "com.typesafe.play" %% "play-json" % "2.9.4",

  // Optional: config file support
  "com.typesafe" % "config" % "1.4.3"
)
