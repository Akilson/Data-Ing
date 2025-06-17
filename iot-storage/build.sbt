name := "iot-storage"

scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  // Kafka client
  "org.apache.kafka" % "kafka-clients" % "3.6.1",

  // JSON support for parsing IoT messages
  "com.typesafe.play" %% "play-json" % "2.9.4",

  // Config file support
  "com.typesafe" % "config" % "1.4.3"
)
