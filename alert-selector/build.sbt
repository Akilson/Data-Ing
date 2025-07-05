name := "alert-selector"

ThisBuild / scalaVersion := "2.13.14"

assembly / assemblyJarName := "alert-selector.jar"
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
  "org.typelevel" %% "cats-effect" % "3.5.4"
)
