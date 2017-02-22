import scalariform.formatter.preferences.{CompactControlReadability, DoubleIndentClassDeclaration, PreserveSpaceBeforeArguments, SpacesAroundMultiImports}
import de.heikoseeberger.sbtheader.HeaderPattern

name := "akka-stream-kafka"

val akkaVersion = "2.4.17"
val kafkaVersion = "0.10.1.1"

val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaVersion

val commonDependencies = Seq(
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-contrib" % "0.6" % Test
)

val coreDependencies = Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  kafkaClients,
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.reactivestreams" % "reactive-streams-tck" % "1.0.0" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "junit" % "junit" % "4.12" % "test",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.12" % "test",
  "org.mockito" % "mockito-core" % "1.10.19" % "test",
  "net.manub" %% "scalatest-embedded-kafka" % "0.11.0" % "test"
    exclude("log4j", "log4j")
)

val commonSettings =
  scalariformSettings ++ Seq(
  organization := "com.typesafe.akka",
  organizationName := "Lightbend",
  startYear := Some(2014),
  test in assembly := {},
  licenses := Seq("Apache License 2.0" -> url("http://opensource.org/licenses/Apache-2.0")),
  scalaVersion := "2.11.8",
  crossScalaVersions := Seq(scalaVersion.value, "2.12.1"),
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

resolvers in ThisBuild ++= Seq(Resolver.bintrayRepo("manub", "maven"))

lazy val root =
  project.in( file(".") )
    .settings(commonSettings)
    .settings(Seq(
    publishArtifact := false,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))))
    .aggregate(core, benchmarks, docs)

lazy val core = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(Seq(
    name := "akka-stream-kafka",
    libraryDependencies ++= commonDependencies ++ coreDependencies
))

lazy val docs = project.in(file("docs"))
  .enablePlugins(ParadoxPlugin)
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka-docs",
    publishArtifact := false,
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    paradoxNavigationDepth := 3,
    paradoxProperties ++= Map(
      "version" -> version.value,
      "akka.version" -> akkaVersion,
      "kafka.version" -> kafkaVersion,
      "scala.binaryVersion"              -> scalaBinaryVersion.value,
      "scala.version"                    -> scalaVersion.value
    )
  )

lazy val Benchmark = config("bench") extend Test

lazy val benchmarks = project
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(DockerPlugin)
  .settings(commonSettings)
  .settings(Seq(
    publishArtifact := false,
    name := "akka-stream-kafka-benchmarks",
    parallelExecution in Benchmark := false,
    libraryDependencies ++= commonDependencies ++ coreDependencies ++ Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
      "io.dropwizard.metrics" % "metrics-core" % "3.1.0",
      "ch.qos.logback" % "logback-classic" % "1.1.3",
      "org.slf4j" % "log4j-over-slf4j" % "1.7.12",
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion
    ),
    dockerfile in docker := {
      val artifact: File = assembly.value
      val artifactTargetPath = s"/app/${artifact.name}"

      new Dockerfile {
        from("netflixoss/java:8")
        add(artifact, artifactTargetPath)
        entryPoint("java", "-jar", artifactTargetPath)
        expose(8080)
      }
    }
  )
  )
  .configs(Benchmark)
  .settings(inConfig(Benchmark)(Defaults.testSettings): _*)
  .dependsOn(core)
