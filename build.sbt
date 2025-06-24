lazy val commonSettings = Seq(
  version := "0.1.0"
)

lazy val iotSimulator = project.in(file("iot-simulator"))
  .settings(commonSettings)
  .settings(
    Compile / mainClass := Some("Main"),
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "3.6.1",
      "org.apache.spark" %% "spark-core" % "3.4.1",
      "org.apache.spark" %% "spark-sql" % "3.4.1", 
      "org.apache.spark" %% "spark-streaming" % "3.4.1",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.4.1",
      "com.typesafe.play" %% "play-json" % "2.9.4",
      "com.typesafe" % "config" % "1.4.3"
    )
  )

lazy val serviceAnalytics = project.in(file("service-analytics"))
  .settings(commonSettings)
  .settings(
    Compile / mainClass := Some("Main")
  )
// lazy val alertSelector  = project.in(file("alert-selector")).settings(commonSettings)
// lazy val alertHandler   = project.in(file("alert-handler")).settings(commonSettings)
// lazy val iotStorage   = project.in(file("iot-storage")).settings(commonSettings)
// lazy val sparkAnalyzer  = project.in(file("spark-analyzer")).settings(commonSettings)

lazy val root = (project in file("."))
  .aggregate(iotSimulator, serviceAnalytics)//, alertSelector, alertHandler, iotStorage, sparkAnalyzer)
  .settings(commonSettings)
