import scalariform.formatter.preferences.{CompactControlReadability, DoubleIndentClassDeclaration, PreserveSpaceBeforeArguments, SpacesAroundMultiImports}
import de.heikoseeberger.sbtheader.HeaderPattern

name := "akka-stream-kafka"

val akkaVersion = "2.4.7"
val kafkaVersion = "0.10.0.0"

val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaVersion

val commonDependencies = Seq(
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
)

val coreDependencies = Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  kafkaClients,
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.reactivestreams" % "reactive-streams-tck" % "1.0.0" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "junit" % "junit" % "4.12" % "test",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.12" % "test",
  "org.mockito" % "mockito-core" % "1.10.19" % "test",
  "net.manub" %% "scalatest-embedded-kafka" % "0.7.0" % "test"
)

val commonSettings =
  scalariformSettings ++ Seq(
  organization := "com.typesafe.akka",
  organizationName := "Lightbend",
  startYear := Some(2014),
  test in assembly := {},
  licenses := Seq("Apache License 2.0" -> url("http://opensource.org/licenses/Apache-2.0")),
  scalaVersion := "2.11.8",
  crossVersion := CrossVersion.binary,
  scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",       // yes, this is 2 args
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture"
),
testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),
  ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(PreserveSpaceBeforeArguments, true)
  .setPreference(CompactControlReadability, true)
  .setPreference(SpacesAroundMultiImports, false),
headers := headers.value ++ Map(
  "scala" -> (
    HeaderPattern.cStyleBlockComment,
    """|/*
       | * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
       | * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
       | */
       |""".stripMargin
  )
))

lazy val root =
  project.in( file(".") )
    .settings(commonSettings)
    .settings(Seq(
    publishArtifact := false,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))))
    .aggregate(core, benchmarks)

lazy val core = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(Seq(
    name := "akka-stream-kafka",
    libraryDependencies ++= commonDependencies ++ coreDependencies
))

lazy val benchmarks = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(Seq(
    publishArtifact := false,
    name := "akka-stream-kafka-benchmarks",
    libraryDependencies ++= commonDependencies ++ coreDependencies ++ Seq("ch.qos.logback" % "logback-classic" % "1.1.3")
  )).dependsOn(core)
