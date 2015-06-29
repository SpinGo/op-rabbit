import sbt._
import Keys._
import spray.boilerplate.BoilerplatePlugin.Boilerplate

import java.util.Properties

object OpRabbit extends Build {
  val akkaVersion = "2.3.10"

  val appProperties = {
    val prop = new Properties()
    IO.load(prop, new File("project/version.properties"))
    prop
  }

  lazy val commonSettings = Seq(
    organization := "com.spingo",
    version := appProperties.getProperty("version"),
    scalaVersion := "2.11.6",
    resolvers ++= Seq(
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "SpinGo OSS" at "http://spingo-oss.s3.amazonaws.com/repositories/releases",
      "Sonatype Releases"  at "http://oss.sonatype.org/content/repositories/releases"
    ),
    libraryDependencies ++= Seq(
      "com.chuusai" %%  "shapeless" % "2.2.3",
      "com.typesafe" % "config" % "1.3.0",
      "com.typesafe.akka"     %%  "akka-actor"   % akkaVersion,
      "com.typesafe.akka"     %%  "akka-testkit" % akkaVersion % "test",
      "com.thenewmotion.akka" %% "akka-rabbitmq" % "1.3.0-SPINGO",
      "ch.qos.logback" % "logback-classic" % "1.1.2",
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "com.spingo" %% "scoped-fixtures" % "1.0.0" % "test"
    ),
    publishMavenStyle := true,
    publishTo := {
      val repo = if (version.value.trim.endsWith("SNAPSHOT")) "snapshots" else "releases"
      Some(repo at s"s3://spingo-oss/repositories/$repo")
    }
  )

  lazy val opRabbit =
    Project(
      id = "op-rabbit",
      base = file("."),
      settings = commonSettings ++ Seq(
        description := "The opinionated Rabbit-MQ plugin",
        name := "op-rabbit"
      ))
      .dependsOn(core)
      .aggregate(core, playJsonSupport, pgChangeSupport, airbrakeLogger, akkaStream, json4sSupport)

  lazy val core =
    Project(id = "core",
      base = file("./core"),
      settings = Boilerplate.settings ++ commonSettings ++ Seq(
        name := "op-rabbit-core"
      ))


  val json4sVersion = "3.2.10"
  lazy val json4sSupport = Project(
    id = "json4s",
    base = file("./addons/json4s"),
    settings = commonSettings ++ Seq(
      name := "op-rabbit-json4s",
      libraryDependencies ++= Seq(
        "org.json4s" %% "json4s-ast"     % json4sVersion,
        "org.json4s" %% "json4s-core"    % json4sVersion,
        "org.json4s" %% "json4s-jackson" % json4sVersion % "provided",
        "org.json4s" %% "json4s-native"  % json4sVersion % "provided")
    ))
    .dependsOn(core)

  lazy val playJsonSupport = Project(
    id = "play-json",
    base = file("./addons/play-json"),
    settings = commonSettings ++ Seq(
      name := "op-rabbit-play-json",
      libraryDependencies += "com.typesafe.play" %% "play-json" % "2.3.0"
    ))
    .dependsOn(core)

  lazy val pgChangeSupport = Project(
    id = "pg-change",
    base = file("./addons/pg-change"),
    settings = commonSettings ++ Seq(
      name := "op-rabbit-pg-change"
    ))
    .dependsOn(core, playJsonSupport)

  lazy val airbrakeLogger = Project(
    id = "airbrake",
    base = file("./addons/airbrake/"),
    settings = commonSettings ++ Seq(
      name := "op-rabbit-airbrake",
      libraryDependencies += "io.airbrake" % "airbrake-java" % "2.2.8"
    ))
    .dependsOn(core)


  lazy val akkaStream = Project(
    id = "akka-stream",
    base = file("./addons/akka-stream"),
    settings = commonSettings ++ Seq(
      name := "op-rabbit-akka-stream",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream-experimental" % "1.0-RC4"),
      unmanagedResourceDirectories in Test ++= Seq(
        (file(".").getAbsoluteFile) / "core" / "src" / "test" / "resources"),
      unmanagedSourceDirectories in Test ++= Seq(
        (file(".").getAbsoluteFile) / "core" / "src" / "test" / "scala" / "com" / "spingo" / "op_rabbit" / "helpers")))
    .dependsOn(core)
}
