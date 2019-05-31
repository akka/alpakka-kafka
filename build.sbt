enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-kafka"

val Scala213 = "2.13.0-RC3"
val akkaVersion = "2.5.23"
val kafkaVersion = "2.1.1"
val kafkaVersionForDocs = "21"
val scalatestVersion = "3.0.8-RC5"
val testcontainersVersion = "1.11.2"
val slf4jVersion = "1.7.26"
val confluentAvroSerializerVersion = "5.0.1"

val kafkaScale = settingKey[Int]("Number of kafka docker containers")

resolvers in ThisBuild ++= Seq(
  // for Embedded Kafka
  Resolver.bintrayRepo("manub", "maven"),
  // for Jupiter interface (JUnit 5)
  Resolver.jcenterRepo
)

val commonSettings = Seq(
  organization := "com.typesafe.akka",
  organizationName := "Lightbend Inc.",
  organizationHomepage := Some(url("https://www.lightbend.com/")),
  homepage := Some(url("https://doc.akka.io/docs/alpakka-kafka/current/")),
  scmInfo := Some(ScmInfo(url("https://github.com/akka/alpakka-kafka"), "git@github.com:akka/alpakka-kafka.git")),
  developers += Developer("contributors",
                          "Contributors",
                          "https://gitter.im/akka/dev",
                          url("https://github.com/akka/alpakka-kafka/graphs/contributors")),
  startYear := Some(2014),
  licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")),
  description := "Alpakka is a Reactive Enterprise Integration library for Java and Scala, based on Reactive Streams and Akka.",
  crossScalaVersions := Seq("2.12.8", "2.11.12", Scala213),
  scalaVersion := "2.12.8",
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
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen"
    ) ++ {
      if (scalaBinaryVersion.value == Scala213) Seq.empty
      else Seq("-Yno-adapted-args", "-Xfuture")
    },
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
  testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  // https://github.com/maichler/sbt-jupiter-interface#framework-options
  // -a Show stack traces and exception class name for AssertionErrors.
  // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
  // -q Suppress stdout for successful tests.
  // -s Try to decode Scala names in stack traces and test names.
  testOptions += Tests.Argument(jupiterTestFramework, "-a", "-v", "-q", "-s"),
  scalafmtOnCompile := true,
  headerLicense := Some(
      HeaderLicense.Custom(
        """|Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
         |Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
         |""".stripMargin
      )
    ),
  bintrayOrganization := Some("akka"),
  bintrayPackage := "alpakka-kafka",
  bintrayRepository := (if (isSnapshot.value) "snapshots" else "maven"),
  projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value)
)

lazy val `alpakka-kafka` =
  project
    .in(file("."))
    .enablePlugins(ScalaUnidocPlugin)
    .disablePlugins(SitePlugin)
    .settings(commonSettings)
    .settings(
      skip in publish := true,
      crossScalaVersions := Nil,
      dockerComposeIgnore := true,
      ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(core, testkit),
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
            |  docs - the sources for generating https://doc.akka.io/docs/alpakka-kafka/current
            |  benchmarks - compare direct Kafka API usage with Alpakka Kafka
            |
            |Useful sbt tasks:
            |
            |  docs/previewSite
            |    builds Paradox and Scaladoc documentation, starts a webserver and
            |    opens a new browser window
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
  .disablePlugins(SitePlugin)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka"),
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream" % akkaVersion,
        "org.apache.kafka" % "kafka-clients" % kafkaVersion
      ),
    mimaPreviousArtifacts := Set(
        organization.value %% name.value % previousStableVersion.value
          .getOrElse(throw new Error("Unable to determine previous version"))
      )
  )

lazy val testkit = project
  .dependsOn(core)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin, SitePlugin)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka-testkit",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka.testkit"),
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion,
        "org.testcontainers" % "kafka" % testcontainersVersion % Provided,
        "org.apache.commons" % "commons-compress" % "1.18", // embedded Kafka pulls in Avro which pulls in commons-compress 1.8.1
        "org.scalatest" %% "scalatest" % scalatestVersion % Provided,
        "junit" % "junit" % "4.12" % Provided,
        "org.junit.jupiter" % "junit-jupiter-api" % JupiterKeys.junitJupiterVersion.value % Provided
      ) ++ {
        if (scalaBinaryVersion.value == Scala213) Seq()
        else
          Seq(
            "org.apache.kafka" %% "kafka" % kafkaVersion exclude ("org.slf4j", "slf4j-log4j12"),
            "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion exclude ("log4j", "log4j")
          )
      },
    Compile / unmanagedSources / excludeFilter := {
      if (scalaBinaryVersion.value == Scala213) {
        HiddenFileFilter ||
        "EmbeddedKafkaLike.scala" ||
        "EmbeddedKafkaTest.java" ||
        "EmbeddedKafkaJunit4Test.java"
      } else (Test / unmanagedSources / excludeFilter).value
    },
    mimaPreviousArtifacts := Set(
        organization.value %% name.value % previousStableVersion.value
          .getOrElse(throw new Error("Unable to determine previous version"))
      )
  )

lazy val tests = project
  .dependsOn(core, testkit)
  .enablePlugins(AutomateHeaderPlugin, DockerCompose, BuildInfoPlugin)
  .disablePlugins(MimaPlugin, SitePlugin)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(Defaults.itSettings)
  .settings(automateHeaderSettings(IntegrationTest))
  .settings(
    name := "akka-stream-kafka-tests",
    libraryDependencies ++= Seq(
        "io.confluent" % "kafka-avro-serializer" % confluentAvroSerializerVersion % Test,
        // See https://github.com/sbt/sbt/issues/3618#issuecomment-448951808
        "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts Artifact("javax.ws.rs-api", "jar", "jar"),
        "org.testcontainers" % "kafka" % testcontainersVersion % Test,
        "org.apache.commons" % "commons-compress" % "1.18", // embedded Kafka pulls in Avro, which pulls in commons-compress 1.8.1, see testing.md
        "org.scalatest" %% "scalatest" % scalatestVersion % Test,
        "io.spray" %% "spray-json" % "1.3.5" % Test,
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.8" % Test, // ApacheV2
        "org.junit.vintage" % "junit-vintage-engine" % JupiterKeys.junitVintageVersion.value % Test,
        // See http://hamcrest.org/JavaHamcrest/distributables#upgrading-from-hamcrest-1x
        "org.hamcrest" % "hamcrest-library" % "2.1" % Test,
        "org.hamcrest" % "hamcrest" % "2.1" % Test,
        "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Test,
        "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
        "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % Test,
        // Schema registry uses Glassfish which uses java.util.logging
        "org.slf4j" % "jul-to-slf4j" % slf4jVersion % Test,
        "org.mockito" % "mockito-core" % "2.24.5" % Test
      ) ++ {
        if (scalaBinaryVersion.value == Scala213) Seq()
        else
          Seq(
            "io.github.embeddedkafka" %% "embedded-kafka-schema-registry" % "5.2.1" % Test exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12")
          )
      } ++
      Seq( // integration test dependencies
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % IntegrationTest,
        "org.scalatest" %% "scalatest" % scalatestVersion % IntegrationTest,
        "com.spotify" % "docker-client" % "8.11.7" % IntegrationTest,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % IntegrationTest,
        "ch.qos.logback" % "logback-classic" % "1.2.3" % IntegrationTest,
        "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % IntegrationTest
      ),
    resolvers += "Confluent Maven Repo" at "https://packages.confluent.io/maven/",
    publish / skip := true,
    whitesourceIgnore := true,
    Test / fork := true,
    Test / parallelExecution := false,
    IntegrationTest / parallelExecution := false,
    Test / unmanagedSources / excludeFilter := {
      if (scalaBinaryVersion.value == Scala213) {
        HiddenFileFilter ||
        "RetentionPeriodSpec.scala" ||
        "IntegrationSpec.scala" ||
        "MultiConsumerSpec.scala" ||
        "ReconnectSpec.scala" ||
        "EmbeddedKafkaSampleSpec.scala" ||
        "TransactionsSpec.scala" ||
        "SerializationSpec.scala" ||
        "PartitionExamples.scala" ||
        "TransactionsExample.scala" ||
        "EmbeddedKafkaWithSchemaRegistryTest.java" ||
        "AssignmentTest.java" ||
        "ProducerExampleTest.java" ||
        "SerializationTest.java" ||
        "TransactionsExampleTest.java"
      } else (Test / unmanagedSources / excludeFilter).value
    },
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
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(BintrayPlugin, MimaPlugin)
  .settings(commonSettings)
  .settings(
    name := "Alpakka Kafka",
    publish / skip := true,
    whitesourceIgnore := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/alpakka-kafka/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Preprocess / preprocessRules := Seq(
        ("\\.java\\.scala".r, _ => ".java")
      ),
    Paradox / siteSubdirName := s"docs/alpakka-kafka/${projectInfoVersion.value}",
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxProperties ++= Map(
        "akka.version" -> akkaVersion,
        "kafka.version" -> kafkaVersion,
        "confluent.version" -> confluentAvroSerializerVersion,
        "scalatest.version" -> scalatestVersion,
        "testcontainers.version" -> testcontainersVersion,
        "extref.akka-docs.base_url" -> s"https://doc.akka.io/docs/akka/$akkaVersion/%s",
        "extref.kafka-docs.base_url" -> s"https://kafka.apache.org/$kafkaVersionForDocs/documentation/%s",
        "extref.java-docs.base_url" -> "https://docs.oracle.com/en/java/javase/11/%s",
        "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/current/",
        "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/$akkaVersion",
        "scaladoc.akka.kafka.base_url" -> {
          val docsHost = sys.env
            .get("CI")
            .map(_ => "https://doc.akka.io")
            .getOrElse(s"http://localhost:${(previewSite / previewFixedPort).value}")
          s"$docsHost/api/alpakka-kafka/${projectInfoVersion.value}/"
        },
        "scaladoc.com.typesafe.config.base_url" -> s"https://lightbend.github.io/config/latest/api/",
        "javadoc.org.apache.kafka.base_url" -> s"https://kafka.apache.org/$kafkaVersionForDocs/javadoc/"
      ),
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifact := makeSite.value -> "www/",
    publishRsyncHost := "akkarepo@gustav.akka.io"
  )

lazy val benchmarks = project
  .dependsOn(core, testkit)
  .enablePlugins(AutomateHeaderPlugin, DockerCompose, BuildInfoPlugin)
  .enablePlugins(DockerPlugin)
  .disablePlugins(SitePlugin)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(Defaults.itSettings)
  .settings(automateHeaderSettings(IntegrationTest))
  .settings(
    crossScalaVersions -= Scala213,
    name := "akka-stream-kafka-benchmarks",
    skip in publish := true,
    whitesourceIgnore := true,
    IntegrationTest / parallelExecution := false,
    libraryDependencies ++= Seq(
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
        "io.dropwizard.metrics" % "metrics-core" % "3.2.6",
        "ch.qos.logback" % "logback-classic" % "1.2.3",
        "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % "it",
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "it",
        "org.scalatest" %% "scalatest" % scalatestVersion % "it"
      ),
    kafkaScale := 1,
    buildInfoPackage := "akka.kafka.benchmarks",
    buildInfoKeys := Seq[BuildInfoKey](kafkaScale),
    dockerComposeTestLogging := false,
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
