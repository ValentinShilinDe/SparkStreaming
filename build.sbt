ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-sql"            % sparkVersion,
  "org.apache.spark" %% "spark-streaming"      % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreaming",
    libraryDependencies ++= sparkDependencies,
    javacOptions ++= Seq("-source", "21"),
    javaOptions ++= Seq(
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    compileOrder := CompileOrder.JavaThenScala
  )
