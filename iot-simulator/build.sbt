name := "iot-simulator"

// Specify the main class
Compile / mainClass := Some("Main")

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.6.1",     // Kafka client
  "org.apache.spark" %% "spark-core" % "3.4.1",        // Spark Core for data processing
  "org.apache.spark" %% "spark-sql" % "3.4.1",         // Spark SQL for structured data processing
  "org.apache.spark" %% "spark-streaming" % "3.4.1",    // Spark Streaming for real-time data processing
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.4.1", // Spark Streaming Kafka integration
  "com.typesafe.play" %% "play-json" % "2.9.4",       // JSON support
  "com.typesafe" % "config" % "1.4.3",                 // Config support 
  // "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5", // Logging support
  // "ch.qos.logback" % "logback-classic" % "1.4.11" // Logback for logging
  
)
