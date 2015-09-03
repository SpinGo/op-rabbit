import spray.boilerplate.BoilerplatePlugin.Boilerplate

import java.util.Properties

val akkaVersion = "2.3.10"

val json4sVersion = "3.2.10"

val appProperties = {
  val prop = new Properties()
  IO.load(prop, new File("project/version.properties"))
  prop
}

val assertNoApplicationConf = taskKey[Unit]("Makes sure application.conf isn't packaged")

val commonSettings = Seq(
  organization := "com.spingo",
  version := appProperties.getProperty("version"),
  scalaVersion := "2.11.7",
  crossScalaVersions := Seq("2.11.7", "2.10.5"),
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
    "com.thenewmotion.akka" %% "akka-rabbitmq" % "1.2.7",
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

lazy val `op-rabbit` = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    description := "The opinionated Rabbit-MQ plugin",
    name := "op-rabbit").
  dependsOn(core).
  aggregate(core, `play-json`, airbrake, `akka-stream`, json4s, `play-json-23`)


lazy val core = (project in file("./core")).
  settings(Boilerplate.settings: _*).
  settings(commonSettings: _*).
  settings(
    name := "op-rabbit-core"
  )


lazy val json4s = (project in file("./addons/json4s")).
  settings(commonSettings: _*).
  settings(
    name := "op-rabbit-json4s",
    libraryDependencies ++= Seq(
      "org.json4s" %% "json4s-ast"     % json4sVersion,
      "org.json4s" %% "json4s-core"    % json4sVersion,
      "org.json4s" %% "json4s-jackson" % json4sVersion % "provided",
      "org.json4s" %% "json4s-native"  % json4sVersion % "provided")).
  dependsOn(core)

lazy val `play-json` = (project in file("./addons/play-json")).
  settings(commonSettings: _*).
  settings(
    name := "op-rabbit-play-json",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.4.2").
  dependsOn(core)

lazy val `play-json-23` = (project in file("./addons/play-json-23")).
  settings(commonSettings: _*).
  settings(
    scalaSource in Compile := `play-json`.base.getAbsoluteFile / "src" / "main" / "scala",
    name := "op-rabbit-play-json-23",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.3.4").
  dependsOn(core)

lazy val airbrake = (project in file("./addons/airbrake/")).
  settings(commonSettings: _*).
  settings(
    name := "op-rabbit-airbrake",
    libraryDependencies += "io.airbrake" % "airbrake-java" % "2.2.8").
  dependsOn(core)


lazy val `akka-stream` = (project in file("./addons/akka-stream")).
  settings(commonSettings: _*).
  settings(
    name := "op-rabbit-akka-stream",
    libraryDependencies ++= Seq(
      "com.timcharper"    %% "acked-stream" % "1.0-RC1",
      "com.typesafe.akka" %% "akka-stream-experimental" % "1.0"),
    unmanagedResourceDirectories in Test ++= Seq(
      (file(".").getAbsoluteFile) / "core" / "src" / "test" / "resources"),
    unmanagedSourceDirectories in Test ++= Seq(
      (file(".").getAbsoluteFile) / "core" / "src" / "test" / "scala" / "com" / "spingo" / "op_rabbit" / "helpers")).
  dependsOn(core)
