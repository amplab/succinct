import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object SuccinctBuild extends Build {

  lazy val root = project.in(file("."))
    .aggregate(core, serde, spark)
    .settings(assemblySettings: _*)
    .settings(commonSettings: _*)
    .settings(TestSettings.settings: _*)
    .dependsOn(core, serde, spark)

  lazy val core = project.in(file("core"))
    .settings(assemblySettings: _*)
    .settings(commonSettings: _*)
    .settings(TestSettings.settings: _*)
    .settings(name := "succinct-core")

  lazy val serde = project.in(file("serde"))
    .settings(assemblySettings: _*)
    .settings(commonSettings: _*)
    .settings(TestSettings.settings: _*)
    .settings(name := "succinct-serde")

  lazy val spark = project.in(file("spark"))
    .settings(assemblySettings: _*)
    .settings(commonSettings: _*)
    .settings(TestSettings.settings: _*)
    .settings(name := "succinct-spark")
    .dependsOn(core)
    .dependsOn(serde)

  lazy val commonSettings = Seq(
    name := "succinct",
    version := "0.1.6-SNAPSHOT",
    organization := "amplab",
    scalaVersion := "2.10.4",

    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test"
    ),

    resolvers ++= Seq(
      "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
      "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
      "Spray" at "http://repo.spray.cc"
    ),

    test in assembly := {},

    mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
        case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
        case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
        case "application.conf" => MergeStrategy.concat
        case "reference.conf" => MergeStrategy.concat
        case "log4j.properties" => MergeStrategy.discard
        case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
        case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
        case _ => MergeStrategy.first
      }
    }
  )
}

object TestSettings {
  lazy val settings = Seq(
    fork := true,
    javaOptions in Test += "-Dspark.driver.allowMultipleContexts=true",
    parallelExecution in Test := false
  )
}
