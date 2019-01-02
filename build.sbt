import Dependencies._
import sbt.Keys.libraryDependencies

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.7",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "ingester",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.elasticsearch" 		% "elasticsearch"           %  "6.5.4",
    libraryDependencies += "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "6.5.4",
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.3"

  )
