import scalariform.formatter.preferences._
import de.heikoseeberger.sbtheader.HeaderPattern

name := "akka-stream-kafka"

val akkaVersion = "2.4.20"
val kafkaVersion = "0.11.0.1"

val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaVersion

val commonDependencies = Seq(
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
)

val coreDependencies = Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  kafkaClients,
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "org.reactivestreams" % "reactive-streams-tck" % "1.0.1" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "junit" % "junit" % "4.12" % Test,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25" % Test,
  "org.mockito" % "mockito-core" % "2.10.0" % Test,
  "net.manub" %% "scalatest-embedded-kafka" % "0.16.0" % Test exclude("log4j", "log4j"),
  "org.apache.kafka" %% "kafka" % kafkaVersion % Test exclude("org.slf4j", "slf4j-log4j12")
)

val commonSettings = Seq(
  organization := "com.typesafe.akka",
  organizationName := "Lightbend",
  startYear := Some(2014),
  test in assembly := {},
  licenses := Seq("Apache License 2.0" -> url("http://opensource.org/licenses/Apache-2.0")),
  scalaVersion := "2.11.11",
  crossScalaVersions := Seq(scalaVersion.value, "2.12.3"),
  crossVersion := CrossVersion.binary,
  scalariformAutoformat := true,
  scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",       // yes, this is 2 args
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture"
),
testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),
  scalariformPreferences := scalariformPreferences.value
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(PreserveSpaceBeforeArguments, true)
  .setPreference(CompactControlReadability, true)
  .setPreference(DanglingCloseParenthesis, Preserve)
  .setPreference(NewlineAtEndOfFile, true)
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
      "version"                          -> version.value,
      "akkaVersion"                      -> akkaVersion,
      "kafkaVersion"                     -> kafkaVersion,
      "scalaVersion"                     -> scalaVersion.value,
      "scalaBinaryVersion"               -> scalaBinaryVersion.value,
      "extref.akka-docs.base_url"        -> s"http://doc.akka.io/docs/akka/$akkaVersion/%s",
      "extref.kafka-docs.base_url"       -> s"https://kafka.apache.org/documentation/%s",
      "scaladoc.akka.base_url"           -> s"http://doc.akka.io/api/akka/$akkaVersion"
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
      "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
      "io.dropwizard.metrics" % "metrics-core" % "3.2.5",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
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
