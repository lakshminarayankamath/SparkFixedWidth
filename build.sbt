name := "SparkFixedWidth"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"
val specs2Version = "3.9.4"
val scalatestVersion = "3.0.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
  "org.specs2"       %% "specs2-core"       % specs2Version % Test,
  "org.specs2"       %% "specs2-junit"      % specs2Version % Test,
  "org.specs2"       %% "specs2-scalacheck" % specs2Version % Test,
  "org.scalatest" %% "scalatest"         % scalatestVersion % Test
)
        