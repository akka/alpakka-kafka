import sbt.Resolver
enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-kafka"

val akkaVersion = "2.5.19"
val kafkaVersion = "2.1.0"
val kafkaVersionForDocs = "21"
val scalatestVersion = "3.0.5"
val junit4Version = "4.12"
val slf4jVersion = "1.7.25"
val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaVersion

val coreDependencies = Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  kafkaClients,
)

val testkitDependencies = Seq(
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion,
  "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" exclude ("log4j", "log4j"),
  "org.apache.commons" % "commons-compress" % "1.18", // embedded Kafka pulls in Avro which pulls in commons-compress 1.8.1
  "org.scalatest" %% "scalatest" % scalatestVersion % Provided,
  "junit" % "junit" % junit4Version % Provided,
  "org.apache.kafka" %% "kafka" % kafkaVersion exclude ("org.slf4j", "slf4j-log4j12")
)

val confluentAvroSerializerVersion = "5.0.1"

val testDependencies = Seq(
  "io.confluent" % "kafka-avro-serializer" % confluentAvroSerializerVersion % Test,
  "net.manub" %% "scalatest-embedded-schema-registry" % "2.0.0" % Test exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12"),
  "org.apache.commons" % "commons-compress" % "1.18", // embedded Kafka pulls in Avro, which pulls in commons-compress 1.8.1, see testing.md 
  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
  "io.spray" %% "spray-json" % "1.3.5" % Test,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.7" % Test, // ApacheV2
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "junit" % "junit" % junit4Version % Test,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
  "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % Test,
  // Schema registry uses Glassfish which uses java.util.logging
  "org.slf4j" % "jul-to-slf4j" % slf4jVersion % Test,
  "org.mockito" % "mockito-core" % "2.23.4" % Test
)

val integrationTestDependencies = Seq(
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % IntegrationTest,
  "org.scalatest" %% "scalatest" % scalatestVersion % IntegrationTest,
  "com.spotify" % "docker-client" % "8.11.7" % IntegrationTest,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % IntegrationTest,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % IntegrationTest,
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25" % IntegrationTest
)

val benchmarkDependencies = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "io.dropwizard.metrics" % "metrics-core" % "3.2.6",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % "it",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "it",
  "org.scalatest" %% "scalatest" % scalatestVersion % "it"
)

val kafkaScale = settingKey[Int]("Number of kafka docker containers")

resolvers in ThisBuild ++= Seq(Resolver.bintrayRepo("manub", "maven"))

val commonSettings = Seq(
  organization := "com.typesafe.akka",
  organizationName := "Lightbend Inc.",
  homepage := Some(url("https://doc.akka.io/docs/akka-stream-kafka/current/")),
  scmInfo := Some(ScmInfo(url("https://github.com/akka/alpakka-kafka"), "git@github.com:akka/alpakka-kafka.git")),
  developers += Developer("contributors",
                          "Contributors",
                          "https://gitter.im/akka/dev",
                          url("https://github.com/akka/alpakka-kafka/graphs/contributors")),
  startYear := Some(2014),
  licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")),
  crossScalaVersions := Seq("2.12.8", "2.11.12"),
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
  scalacOptions in (Compile, doc) := scalacOptions.value ++ Seq(
    "-doc-title",
    "Alpakka Kafka",
    "-doc-version",
    version.value,
    "-sourcepath",
    (baseDirectory in ThisBuild).value.toString,
    "-doc-source-url", {
      val branch = if (isSnapshot.value) "master" else s"v${version.value}"
      s"https://github.com/akka/alpakka-kafka/tree/${branch}€{FILE_PATH}.scala#L1"
    },
    "-skip-packages",
    "akka.pattern" // for some reason Scaladoc creates this
  ),
  // show full stack traces and test case durations
  testOptions += Tests.Argument("-oDF"),
  // -a Show stack traces and exception class name for AssertionErrors.
  // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
  // -q Suppress stdout for successful tests.
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-q"),
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
            |The build has three main modules:
            |  core - the Kafka connector sources
            |  tests - tests, Docker based integration tests, code for the documentation
            |  testkit - framework for testing the connector
            |
            |Other modules:
            |  docs - the sources for generating https://doc.akka.io/docs/akka-stream-kafka/current
            |  benchmarks - compare direct Kafka API usage with Alpakka Kafka
            |
            |Useful sbt tasks:
            |
            |  docs/Local/paradox
            |    builds documentation, which is generated at
            |    docs/target/paradox/site/local/index.html
            |
            |  test
            |    runs all the tests
            |
            |  tests/dockerComposeTest it:test
            |    run integration test backed by Docker containers
            |
            |  benchmarks/dockerComposeTest it:testOnly *.AlpakkaKafkaPlainConsumer
            |    run a single benchmark backed by Docker containers
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
    mimaPreviousArtifacts := Set(
      organization.value %% name.value % previousStableVersion.value
        .getOrElse(throw new Error("Unable to determine previous version"))
    )
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
    mimaPreviousArtifacts := Set(
      organization.value %% name.value % previousStableVersion.value
        .getOrElse(throw new Error("Unable to determine previous version"))
    )
  )

lazy val tests = project
  .dependsOn(core, testkit)
  .enablePlugins(AutomateHeaderPlugin, DockerCompose, BuildInfoPlugin)
  .disablePlugins(MimaPlugin)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(Defaults.itSettings)
  .settings(automateHeaderSettings(IntegrationTest))
  .settings(
    name := "akka-stream-kafka-tests",
    libraryDependencies ++= testDependencies ++ integrationTestDependencies,
    resolvers += "Confluent Maven Repo" at "https://packages.confluent.io/maven/",
    publish / skip := true,
    whitesourceIgnore := true,
    Test / fork := true,
    Test / parallelExecution := false,
    IntegrationTest / parallelExecution := false,
    kafkaScale := 3,
    buildInfoPackage := "akka.kafka",
    buildInfoKeys := Seq[BuildInfoKey](kafkaScale),
    dockerComposeTestLogging := true,
    dockerComposeFilePath := (baseDirectory.value / ".." / "docker-compose.yml").getAbsolutePath,
    dockerComposeTestCommandOptions := {
      import com.github.ehsanyou.sbt.docker.compose.commands.test._
      DockerComposeTestCmd(DockerComposeTest.ItTest)
        .withEnvVar("KAFKA_SCALE", kafkaScale.value.toString)
    }
  )

commands += Command.command("dockerComposeTestAll") { state ⇒
  val extracted = Project.extract(state)
  val (_, allTests) = extracted.runTask(tests / IntegrationTest / definedTestNames, state)
  allTests.map(test => s"tests/dockerComposeTest it:testOnly $test").foldRight(state)(_ :: _)
}

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(AkkaParadoxPlugin)
  .settings(commonSettings)
  .settings(
    name := "Alpakka Kafka",
    skip in publish := true,
    whitesourceIgnore := true,
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxProperties ++= Map(
      "project.url" -> "https://doc.akka.io/docs/akka-stream-kafka/current/",
      "akka.version" -> akkaVersion,
      "kafka.version" -> kafkaVersion,
      "confluent.version" -> confluentAvroSerializerVersion,
      "scalatest.version" -> scalatestVersion,
      "extref.akka-docs.base_url" -> s"https://doc.akka.io/docs/akka/$akkaVersion/%s",
      "extref.kafka-docs.base_url" -> s"https://kafka.apache.org/$kafkaVersionForDocs/documentation/%s",
      "extref.java-docs.base_url" -> "https://docs.oracle.com/en/java/javase/11/%s",
      "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/current/",
      "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/$akkaVersion",
      "scaladoc.akka.kafka.base_url" -> s"https://doc.akka.io/api/akka-stream-kafka/${version.value}/",
      "scaladoc.com.typesafe.config.base_url" -> s"https://lightbend.github.io/config/latest/api/",
      "javadoc.org.apache.kafka.base_url" -> s"https://kafka.apache.org/$kafkaVersionForDocs/javadoc/"
    ),
    resolvers += Resolver.jcenterRepo,
    paradoxLocalApiKey := "scaladoc.akka.kafka.base_url",
    paradoxLocalApiDir := (core / Compile / doc).value,
  )

lazy val benchmarks = project
  .dependsOn(core, testkit)
  .enablePlugins(AutomateHeaderPlugin, DockerCompose, BuildInfoPlugin)
  .enablePlugins(DockerPlugin)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(Defaults.itSettings)
  .settings(automateHeaderSettings(IntegrationTest))
  .settings(
    name := "akka-stream-kafka-benchmarks",
    skip in publish := true,
    whitesourceIgnore := true,
    IntegrationTest / parallelExecution := false,
    libraryDependencies ++= benchmarkDependencies,
    kafkaScale := 1,
    buildInfoPackage := "akka.kafka.benchmarks",
    buildInfoKeys := Seq[BuildInfoKey](kafkaScale),
    dockerComposeTestLogging := true,
    dockerComposeFilePath := (baseDirectory.value / ".." / "docker-compose.yml").getAbsolutePath,
    dockerComposeTestCommandOptions := {
      import com.github.ehsanyou.sbt.docker.compose.commands.test._
      DockerComposeTestCmd(DockerComposeTest.ItTest)
        .withEnvVar("KAFKA_SCALE", kafkaScale.value.toString)
    },
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
