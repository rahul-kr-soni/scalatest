ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "yarn_arch_practice_10"
  )
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0-preview2"