enablePlugins(AutomateHeaderPlugin)

import scalariform.formatter.preferences._

name := "akka-stream-kafka"

val akkaVersion = "2.5.13"
val kafkaVersion = "1.0.1"
val kafkaVersionForDocs = "10"
val scalatestVersion = "3.0.4"

val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaVersion

val commonDependencies = Seq(
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
)

val coreDependencies = Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  kafkaClients,
  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
  "org.reactivestreams" % "reactive-streams-tck" % "1.0.1" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "junit" % "junit" % "4.12" % Test,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25" % Test,
  "org.mockito" % "mockito-core" % "2.15.0" % Test,
  "net.manub" %% "scalatest-embedded-kafka" % "1.0.0" % Test exclude("log4j", "log4j"),
  "org.apache.kafka" %% "kafka" % kafkaVersion % Test exclude("org.slf4j", "slf4j-log4j12")
)

resolvers in ThisBuild ++= Seq(Resolver.bintrayRepo("manub", "maven"))

val docDependencies = Seq(
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25"
).map(_ % Test)

val commonSettings = Seq(
  organization := "com.typesafe.akka",
  organizationName := "Lightbend Inc.",
  homepage := Some(url("https://github.com/akka/reactive-kafka")),
  scmInfo := Some(ScmInfo(url("https://github.com/akka/reactive-kafka"), "git@github.com:akka/reactive-kafka.git")),
  developers += Developer("contributors",
    "Contributors",
    "https://gitter.im/akka/dev",
    url("https://github.com/akka/reactive-kafka/graphs/contributors")),
  startYear := Some(2014),
  licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")),
  crossScalaVersions := Seq("2.12.6", "2.11.12"),
  scalaVersion := crossScalaVersions.value.head,
  crossVersion := CrossVersion.binary,
  scalariformAutoformat := true,
  javacOptions ++= Seq(
    "-Xlint:deprecation"
  ),
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
  testOptions += Tests.Argument("-oD"),
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),
  scalariformPreferences := scalariformPreferences.value
    .setPreference(DoubleIndentConstructorArguments, true)
    .setPreference(PreserveSpaceBeforeArguments, true)
    .setPreference(CompactControlReadability, true)
    .setPreference(DanglingCloseParenthesis, Preserve)
    .setPreference(NewlineAtEndOfFile, true)
    .setPreference(SpacesAroundMultiImports, false),
  headerLicense := Some(HeaderLicense.Custom(
    """|Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
       |Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
       |""".stripMargin
  )),
  bintrayOrganization := Some("akka"),
  bintrayPackage := "alpakka-kafka",
  bintrayRepository := (if (isSnapshot.value) "snapshots" else "maven"),
)

lazy val `alpakka-kafka` =
  project.in(file("."))
    .settings(commonSettings)
    .settings(
      skip in publish := true,
      dockerComposeIgnore := true,
      onLoadMessage :=
          """
            |** Welcome to the Alpakka Kafka connector! **
            |
            |The build has three modules
            |  core - the Kafka connector sources and tests
            |  docs - the sources for generating https://doc.akka.io/docs/akka-stream-kafka/current
            |  benchmarks - for instrunctions read benchmarks/README.md
            |
            |Useful sbt tasks:
            |
            |  docs/Local/paradox - builds documentation, which is generated at
            |    docs/target/paradox/site/local/home.html
            |
            |  test - runs all the tests
          """.stripMargin
    )
    .aggregate(core, benchmarks, docs)

lazy val core = project
  .enablePlugins(AutomateHeaderPlugin, DockerCompose)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka"),
    Test/fork := true,
    Test/parallelExecution := false,
    libraryDependencies ++= commonDependencies ++ coreDependencies ++ Seq(
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "it",
      "org.scalatest" %% "scalatest" % scalatestVersion % "it",
      "com.spotify" % "docker-client" % "8.11.5" % "it",
    ),
    mimaPreviousArtifacts := (20 to 20).map(minor => organization.value %% name.value % s"0.$minor").toSet,
  )
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)

lazy val docs = project.in(file("docs"))
  .enablePlugins(ParadoxPlugin)
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka-docs",
    skip in publish := true,
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    paradoxNavigationDepth := 3,
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxProperties ++= Map(
      "version"                               -> version.value,
      "akkaVersion"                           -> akkaVersion,
      "kafkaVersion"                          -> kafkaVersion,
      "scalaVersion"                          -> scalaVersion.value,
      "scalaBinaryVersion"                    -> scalaBinaryVersion.value,
      "extref.akka-docs.base_url"             -> s"https://doc.akka.io/docs/akka/$akkaVersion/%s",
      "extref.kafka-docs.base_url"            -> s"https://kafka.apache.org/$kafkaVersionForDocs/documentation/%s",
      "scaladoc.scala.base_url"               -> s"https://www.scala-lang.org/api/current/",
      "scaladoc.akka.base_url"                -> s"https://doc.akka.io/api/akka/$akkaVersion",
      "scaladoc.akka.kafka.base_url"          -> s"https://doc.akka.io/api/akka-stream-kafka/${version.value}/",
      "scaladoc.com.typesafe.config.base_url" -> s"https://lightbend.github.io/config/latest/api/",
      "javadoc.org.apache.kafka.base_url"     -> s"https://kafka.apache.org/$kafkaVersionForDocs/javadoc/"
    ),
    paradoxLocalApiKey := "scaladoc.akka.kafka.base_url",
    paradoxLocalApiDir := (core / Compile / doc).value,
    libraryDependencies ++= docDependencies
  )

lazy val Benchmark = config("bench") extend Test

lazy val benchmarks = project
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(DockerPlugin)
  .settings(commonSettings)
  .settings(
    skip in publish := true,
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
  .configs(Benchmark)
  .settings(inConfig(Benchmark)(Defaults.testSettings): _*)
  .dependsOn(core)
