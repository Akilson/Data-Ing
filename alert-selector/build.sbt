name := "alert-selector"

ThisBuild / scalaVersion := "2.13.14"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "3.5.1",
  "io.circe" %% "circe-core" % "0.14.7",
  "io.circe" %% "circe-parser" % "0.14.7",
  "org.typelevel" %% "cats-effect" % "3.5.4"
)
