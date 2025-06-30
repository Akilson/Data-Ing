name := "drone-simulator"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "3.6.1",            // Kafka client
  "org.apache.kafka" % "kafka-clients" % "3.6.1",
  "com.typesafe" % "config" % "1.4.3"                  // optional config support
)