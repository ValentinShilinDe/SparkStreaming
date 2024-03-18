ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"

lazy val circeDependencies = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % "0.11.2")

lazy val sparkDependencies = Seq(
  "org.apache.spark"         %% "spark-sql"            % sparkVersion,
  "org.apache.spark"         %% "spark-streaming"      % sparkVersion,
  "org.apache.spark"         %% "spark-sql-kafka-0-10" % "3.2.0",
  "org.apache.logging.log4j" % "log4j-core"            % "2.20.0",
  "io.netty"          % "netty-all"                   % "4.1.97.Final"
)

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreaming",
    libraryDependencies ++= sparkDependencies ++ circeDependencies,
    javacOptions ++= Seq("-source", "1.8"),
    compileOrder := CompileOrder.JavaThenScala
  )