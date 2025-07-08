name := "service-analytics"
fork := true

ThisBuild / scalaVersion := "2.13.14"


javaOptions ++= Seq(
  "-XX:+IgnoreUnrecognizedVMOptions",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false"
)



libraryDependencies ++= Seq(
  // Spark for data processing
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1",
  
  // AWS S3/Minio integration
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.261",
  
  // JSON processing
  "com.typesafe.play" %% "play-json" % "2.9.4",
  
  // HTTP client for Grafana API
  "com.softwaremill.sttp.client3" %% "core" % "3.8.13",
  "com.softwaremill.sttp.client3" %% "play-json" % "3.8.13",
  
  // InfluxDB client (for Grafana data source)
  "com.influxdb" % "influxdb-client-java" % "6.8.0",
  
  // Configuration
  "com.typesafe" % "config" % "1.4.3",
  
  // Logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "ch.qos.logback" % "logback-classic" % "1.4.11",
)
