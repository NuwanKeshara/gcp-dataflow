ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "gcp-dataflow",
    version := "0.1",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.5",
      "org.apache.spark" %% "spark-sql"  % "3.5.5",
      "org.postgresql"    % "postgresql" % "42.7.4"
    )
  )
