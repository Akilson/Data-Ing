name := "alert-handler"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.6.1",     // Kafka client
  "com.typesafe.play" %% "play-json" % "2.9.4",       // JSON support
  "com.typesafe" % "config" % "1.4.3"                 // Config support
)
