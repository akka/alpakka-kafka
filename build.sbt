enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-kafka"

val akkaVersion = "2.5.14"
val kafkaVersion = "2.0.0"
val kafkaVersionForDocs = "20"
val scalatestVersion = "3.0.4"
val slf4jVersion = "1.7.25"

val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaVersion

val coreDependencies = Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  kafkaClients,
)

val testkitDependencies = Seq(
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion,
  "net.manub" %% "scalatest-embedded-kafka" % "1.0.0" exclude ("log4j", "log4j"),
  "org.apache.kafka" %% "kafka" % kafkaVersion exclude ("org.slf4j", "slf4j-log4j12")
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
  "org.reactivestreams" % "reactive-streams-tck" % "1.0.1" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "junit" % "junit" % "4.12" % Test,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
  "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % Test,
  "org.mockito" % "mockito-core" % "2.15.0" % Test
)

val integrationTestDependencies = Seq(
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % IntegrationTest,
  "org.scalatest" %% "scalatest" % scalatestVersion % IntegrationTest,
  "com.spotify" % "docker-client" % "8.11.5" % IntegrationTest,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % IntegrationTest,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % IntegrationTest,
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25" % IntegrationTest
)

resolvers in ThisBuild ++= Seq(Resolver.bintrayRepo("manub", "maven"))

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
  javacOptions ++= Seq(
    "-Xlint:deprecation"
  ),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "UTF-8", // yes, this is 2 args
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
  scalafmtOnCompile := true,
  headerLicense := Some(
    HeaderLicense.Custom(
      """|Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
         |Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
         |""".stripMargin
    )
  ),
  bintrayOrganization := Some("akka"),
  bintrayPackage := "alpakka-kafka",
  bintrayRepository := (if (isSnapshot.value) "snapshots" else "maven"),
)

lazy val `alpakka-kafka` =
  project
    .in(file("."))
    .settings(commonSettings)
    .settings(
      skip in publish := true,
      dockerComposeIgnore := true,
      onLoadMessage :=
        """
            |** Welcome to the Alpakka Kafka connector! **
            |
            |The build has three modules
            |  core - the Kafka connector sources
            |  tests - tests, Docker based integration tests, code for the documentation
            |  testkit - framework for testing the connector
            |
            |  docs - the sources for generating https://doc.akka.io/docs/akka-stream-kafka/current
            |  benchmarks - for instructions read benchmarks/README.md
            |
            |Useful sbt tasks:
            |
            |  docs/Local/paradox - builds documentation, which is generated at
            |    docs/target/paradox/site/local/home.html
            |
            |  test - runs all the tests
            |  tests/dockerComposeTest it:test --scale kafka=3
            |    - run integration test backed by Docker containers
          """.stripMargin
    )
    .aggregate(core, testkit, tests, benchmarks, docs)

lazy val core = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka"),
    libraryDependencies ++= coreDependencies,
    mimaPreviousArtifacts := previousStableVersion.value.map(organization.value %% name.value % _).toSet
  )

lazy val testkit = project
  .dependsOn(core)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka-testkit",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka.testkit"),
    libraryDependencies ++= testkitDependencies,
    mimaPreviousArtifacts := previousStableVersion.value.map(organization.value %% name.value % _).toSet
  )

lazy val tests = project
  .dependsOn(core, testkit)
  .enablePlugins(AutomateHeaderPlugin, DockerCompose)
  .disablePlugins(MimaPlugin)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(Defaults.itSettings)
  .settings(automateHeaderSettings(IntegrationTest))
  .settings(
    name := "akka-stream-kafka-tests",
    libraryDependencies ++= testDependencies ++ integrationTestDependencies,
    publish / skip := true,
    Test / fork := true,
    Test / parallelExecution := false,
    dockerComposeFilePath := (baseDirectory.value / ".." / "docker-compose.yml").getAbsolutePath
  )

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(ParadoxPlugin)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka-docs",
    skip in publish := true,
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    paradoxNavigationDepth := 3,
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxProperties ++= Map(
      "project.url" -> "https://doc.akka.io/docs/akka-stream-kafka/current/",
      "version" -> version.value,
      "akkaVersion" -> akkaVersion,
      "kafkaVersion" -> kafkaVersion,
      "scalaVersion" -> scalaVersion.value,
      "scalaBinaryVersion" -> scalaBinaryVersion.value,
      "extref.akka-docs.base_url" -> s"https://doc.akka.io/docs/akka/$akkaVersion/%s",
      "extref.kafka-docs.base_url" -> s"https://kafka.apache.org/$kafkaVersionForDocs/documentation/%s",
      "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/current/",
      "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/$akkaVersion",
      "scaladoc.akka.kafka.base_url" -> s"https://doc.akka.io/api/akka-stream-kafka/${version.value}/",
      "scaladoc.com.typesafe.config.base_url" -> s"https://lightbend.github.io/config/latest/api/",
      "javadoc.org.apache.kafka.base_url" -> s"https://kafka.apache.org/$kafkaVersionForDocs/javadoc/"
    ),
    paradoxLocalApiKey := "scaladoc.akka.kafka.base_url",
    paradoxLocalApiDir := (core / Compile / doc).value,
  )

lazy val Benchmark = config("bench") extend Test

lazy val benchmarks = project
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(DockerPlugin)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka-benchmarks",
    skip in publish := true,
    parallelExecution in Benchmark := false,
    libraryDependencies ++= coreDependencies ++ Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
      "io.dropwizard.metrics" % "metrics-core" % "3.2.5",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
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
