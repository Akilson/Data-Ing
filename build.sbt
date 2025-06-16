lazy val commonSettings = Seq(
  scalaVersion := "2.13.12",
  version := "0.1.0"
)

lazy val iotSimulator = project.in(file("iot-simulator")).settings(commonSettings)
lazy val alertSelector  = project.in(file("alert-selector")).settings(commonSettings)
lazy val alertHandler   = project.in(file("alert-handler")).settings(commonSettings)
lazy val iotStorage   = project.in(file("iot-storage")).settings(commonSettings)
lazy val sparkAnalyzer  = project.in(file("spark-analyzer")).settings(commonSettings)

lazy val root = (project in file("."))
  .aggregate(iotSimulator, alertSelector, alertHandler, iotStorage, sparkAnalyzer)
  .settings(commonSettings)
