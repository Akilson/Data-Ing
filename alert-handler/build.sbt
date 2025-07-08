name := "alert-handler"

assembly / assemblyJarName := "alert-handler.jar"
mainClass in assembly := Some("Main")

// Optionally: Merge strategy for duplicate files (especially META-INF issues)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "3.5.1",
  "io.circe" %% "circe-core" % "0.14.7",
  "io.circe" %% "circe-parser" % "0.14.7",
  "com.twilio.sdk" % "twilio" % "9.8.0",
  "com.softwaremill.sttp.client3" %% "core" % "3.8.11",
  "io.circe" %% "circe-generic" % "0.14.5",
)
