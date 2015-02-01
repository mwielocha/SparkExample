import sbt._
import sbt.Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object SparkExample extends Build {

  val appName         = "SparkExample"
  val appVersion      = "0.0.1-SNAPSHOT"

  val appDependencies = Seq(
    "org.apache.spark" % "spark-core_2.10" % "1.2.0" withSources(),
    "com.typesafe.play" %% "play-json" % "2.2.1",
    "com.typesafe" % "config" % "1.2.1",
    "org.scalaz" %% "scalaz-core" % "7.1.0",
    "commons-cli" % "commons-cli" % "1.2",
    "org.specs2" %% "specs2" % "2.4.1" % "test"
  )

  val buildSettings = Defaults.defaultSettings ++ Seq(
    resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    version := appVersion,
    organization := "com.opigram",
    exportJars := true,
    scalaVersion := "2.10.4",
    test in assembly := { }
  )

  val main = Project(id = appName, base = file("."),
    settings = assemblySettings ++ buildSettings ++ Seq(
      libraryDependencies ++= appDependencies
    )
  )
}
