name := "alert-handler"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "3.5.1",
  "io.circe" %% "circe-core" % "0.14.7",
  "io.circe" %% "circe-parser" % "0.14.7",
)
