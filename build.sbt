import com.typesafe.tools.mima.core.{Problem, ProblemFilters}

enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-kafka"

val Nightly = sys.env.get("EVENT_NAME").contains("schedule")

// align ignore-prefixes in scripts/link-validator.conf
val Scala213 = "2.13.8"

val AkkaBinaryVersionForDocs = "2.6"
val akkaVersion = "2.6.19"

// Keep .scala-steward.conf pin in sync
val kafkaVersion = "3.0.1"
val KafkaVersionForDocs = "30"
// This should align with the ScalaTest version used in the Akka 2.6.x testkit
// https://github.com/akka/akka/blob/main/project/Dependencies.scala#L41
val scalatestVersion = "3.1.4"
val testcontainersVersion = "1.16.3"
val slf4jVersion = "1.7.36"
// this depends on Kafka, and should be upgraded to such latest version
// that depends on the same Kafka version, as is defined above
// See https://mvnrepository.com/artifact/io.confluent/kafka-avro-serializer?repo=confluent-packages
val confluentAvroSerializerVersion = "7.0.3"
val confluentLibsExclusionRules = Seq(
  ExclusionRule("log4j", "log4j"),
  ExclusionRule("org.slf4j", "slf4j-log4j12"),
  ExclusionRule("com.typesafe.scala-logging"),
  ExclusionRule("org.apache.kafka")
)

ThisBuild / resolvers ++= Seq(
  // for Jupiter interface (JUnit 5)
  Resolver.jcenterRepo
)

TaskKey[Unit]("verifyCodeFmt") := {
  javafmtCheckAll.all(ScopeFilter(inAnyProject)).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted Java code found. Please run 'javafmtAll' and commit the reformatted code"
    )
  }
  scalafmtCheckAll.all(ScopeFilter(inAnyProject)).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted Scala code found. Please run 'scalafmtAll' and commit the reformatted code"
    )
  }
  (Compile / scalafmtSbtCheck).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted sbt code found. Please run 'scalafmtSbt' and commit the reformatted code"
    )
  }
}

addCommandAlias("verifyCodeStyle", "headerCheck; verifyCodeFmt")
addCommandAlias("verifyDocs", ";+doc ;unidoc ;docs/paradoxBrowse")

val commonSettings = Def.settings(
  organization := "com.typesafe.akka",
  organizationName := "Lightbend Inc.",
  organizationHomepage := Some(url("https://www.lightbend.com/")),
  homepage := Some(url("https://doc.akka.io/docs/alpakka-kafka/current")),
  scmInfo := Some(ScmInfo(url("https://github.com/akka/alpakka-kafka"), "git@github.com:akka/alpakka-kafka.git")),
  developers += Developer("contributors",
                          "Contributors",
                          "",
                          url("https://github.com/akka/alpakka-kafka/graphs/contributors")),
  startYear := Some(2014),
  licenses := Seq("Apache-2.0" -> url("https://opensource.org/licenses/Apache-2.0")),
  description := "Alpakka is a Reactive Enterprise Integration library for Java and Scala, based on Reactive Streams and Akka.",
  crossScalaVersions := Seq(Scala213),
  scalaVersion := Scala213,
  crossVersion := CrossVersion.binary,
  javacOptions ++= Seq(
      "-Xlint:deprecation",
      "-Xlint:unchecked"
    ),
  scalacOptions ++= Seq(
      "-encoding",
      "UTF-8", // yes, this is 2 args
      "-Wconf:cat=feature:w,cat=deprecation:w,cat=unchecked:w,cat=lint:w,cat=unused:w,cat=w-flag:w"
    ) ++ {
      if (insideCI.value && !Nightly) Seq("-Werror")
      else Seq.empty
    },
  Compile / doc / scalacOptions := scalacOptions.value ++ Seq(
      "-Wconf:cat=scaladoc:i",
      "-doc-title",
      "Alpakka Kafka",
      "-doc-version",
      version.value,
      "-sourcepath",
      (ThisBuild / baseDirectory).value.toString,
      "-skip-packages",
      "akka.pattern:scala", // for some reason Scaladoc creates this
      "-doc-source-url", {
        val branch = if (isSnapshot.value) "master" else s"v${version.value}"
        s"https://github.com/akka/alpakka-kafka/tree/${branch}€{FILE_PATH_EXT}#L€{FILE_LINE}"
      },
      "-doc-canonical-base-url",
      "https://doc.akka.io/api/alpakka-kafka/current/"
    ),
  Compile / doc / scalacOptions -= "-Xfatal-warnings",
  // show full stack traces and test case durations
  testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  // https://github.com/maichler/sbt-jupiter-interface#framework-options
  // -a Show stack traces and exception class name for AssertionErrors.
  // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
  // -q Suppress stdout for successful tests.
  // -s Try to decode Scala names in stack traces and test names.
  testOptions += Tests.Argument(jupiterTestFramework, "-a", "-v", "-q", "-s"),
  scalafmtOnCompile := false,
  javafmtOnCompile := false,
  ThisBuild / mimaReportSignatureProblems := true,
  headerLicense := Some(
      HeaderLicense.Custom(
        """|Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
           |Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
           |""".stripMargin
      )
    ),
  projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value),
  sonatypeProfileName := "com.typesafe"
)

lazy val `alpakka-kafka` =
  project
    .in(file("."))
    .enablePlugins(ScalaUnidocPlugin)
    .disablePlugins(SitePlugin, MimaPlugin)
    .settings(commonSettings)
    .settings(
      publish / skip := true,
      // TODO: add clusterSharding to unidocProjectFilter when we drop support for Akka 2.5
      ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(core, testkit),
      onLoadMessage :=
        """
            |** Welcome to the Alpakka Kafka connector! **
            |
            |The build has three main modules:
            |  core - the Kafka connector sources
            |  clusterSharding - Akka Cluster External Sharding with Alpakka Kafka
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
            |  verifyCodeStyle
            |    checks if all of the code is formatted according to the configuration
            |
            |  verifyDocs
            |    builds all of the docs
            |
            |  test
            |    runs all the tests
            |
            |  tests/IntegrationTest/test
            |    run integration tests backed by Docker containers
            |
            |  tests/testOnly -- -t "A consume-transform-produce cycle must complete in happy-path scenario"
            |    run a single test with an exact name (use -z for partial match)
            |
            |  benchmarks/IntegrationTest/testOnly *.AlpakkaKafkaPlainConsumer
            |    run a single benchmark backed by Docker containers
          """.stripMargin
    )
    .aggregate(core, testkit, clusterSharding, tests, benchmarks, docs)

lazy val core = project
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(SitePlugin)
  .settings(commonSettings)
  .settings(VersionGenerator.settings)
  .settings(
    name := "akka-stream-kafka",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka"),
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream" % akkaVersion,
        "com.typesafe.akka" %% "akka-discovery" % akkaVersion % Provided,
        "org.apache.kafka" % "kafka-clients" % kafkaVersion
      ),
    mimaPreviousArtifacts := Set(
        organization.value %% name.value % previousStableVersion.value
          .getOrElse(throw new Error("Unable to determine previous version"))
      ),
    mimaBinaryIssueFilters += ProblemFilters.exclude[Problem]("akka.kafka.internal.*")
  )

lazy val testkit = project
  .dependsOn(core)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(SitePlugin)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka-testkit",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka.testkit"),
    JupiterKeys.junitJupiterVersion := "5.8.2",
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion,
        "org.testcontainers" % "kafka" % testcontainersVersion % Provided,
        "org.scalatest" %% "scalatest" % scalatestVersion % Provided,
        "junit" % "junit" % "4.13.2" % Provided,
        "org.junit.jupiter" % "junit-jupiter-api" % JupiterKeys.junitJupiterVersion.value % Provided
      ),
    mimaPreviousArtifacts := Set(
        organization.value %% name.value % previousStableVersion.value
          .getOrElse(throw new Error("Unable to determine previous version"))
      ),
    mimaBinaryIssueFilters += ProblemFilters.exclude[Problem]("akka.kafka.testkit.internal.*")
  )

lazy val clusterSharding = project
  .in(file("./cluster-sharding"))
  .dependsOn(core)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(SitePlugin)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka-cluster-sharding",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka.cluster.sharding"),
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion
      ),
    mimaPreviousArtifacts := Set(
        organization.value %% name.value % previousStableVersion.value
          .getOrElse(throw new Error("Unable to determine previous version"))
      )
  )

lazy val tests = project
  .dependsOn(core, testkit, clusterSharding)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin, SitePlugin)
  .configs(IntegrationTest.extend(Test))
  .settings(commonSettings)
  .settings(Defaults.itSettings)
  .settings(headerSettings(IntegrationTest))
  .settings(
    name := "akka-stream-kafka-tests",
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
        "com.google.protobuf" % "protobuf-java" % "3.19.1", // use the same version as in scalapb
        "io.confluent" % "kafka-avro-serializer" % confluentAvroSerializerVersion % Test excludeAll (confluentLibsExclusionRules: _*),
        // See https://github.com/sbt/sbt/issues/3618#issuecomment-448951808
        "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts Artifact("javax.ws.rs-api", "jar", "jar"),
        "org.testcontainers" % "kafka" % testcontainersVersion % Test,
        "org.scalatest" %% "scalatest" % scalatestVersion % Test,
        "io.spray" %% "spray-json" % "1.3.6" % Test,
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.2.2" % Test, // ApacheV2
        "org.junit.vintage" % "junit-vintage-engine" % JupiterKeys.junitVintageVersion.value % Test,
        // See http://hamcrest.org/JavaHamcrest/distributables#upgrading-from-hamcrest-1x
        "org.hamcrest" % "hamcrest-library" % "2.2" % Test,
        "org.hamcrest" % "hamcrest" % "2.2" % Test,
        "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Test,
        "ch.qos.logback" % "logback-classic" % "1.2.11" % Test,
        "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % Test,
        // Schema registry uses Glassfish which uses java.util.logging
        "org.slf4j" % "jul-to-slf4j" % slf4jVersion % Test,
        "org.mockito" % "mockito-core" % "4.5.1" % Test,
        "com.thesamet.scalapb" %% "scalapb-runtime" % "0.10.11" % Test
      ),
    resolvers ++= Seq(
        "Confluent Maven Repo" at "https://packages.confluent.io/maven/"
      ),
    publish / skip := true,
    Test / fork := true,
    Test / parallelExecution := false,
    IntegrationTest / parallelExecution := false
  )

lazy val docs = project
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(MimaPlugin)
  .settings(commonSettings)
  .settings(
    name := "Alpakka Kafka",
    publish / skip := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/alpakka-kafka/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Preprocess / preprocessRules := Seq(
        ("\\.java\\.scala".r, _ => ".java"),
        ("https://javadoc\\.io/page/".r, _ => "https://javadoc\\.io/static/"),
        // bug in Scaladoc
        ("https://docs\\.oracle\\.com/en/java/javase/11/docs/api/java.base/java/time/Duration\\$.html".r,
         _ => "https://docs\\.oracle\\.com/en/java/javase/11/docs/api/java.base/java/time/Duration.html"),
        // Add Java module name https://github.com/ThoughtWorksInc/sbt-api-mappings/issues/58
        ("https://docs\\.oracle\\.com/en/java/javase/11/docs/api/".r,
         _ => "https://docs\\.oracle\\.com/en/java/javase/11/docs/api/")
      ),
    Paradox / siteSubdirName := s"docs/alpakka-kafka/${projectInfoVersion.value}",
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxProperties ++= Map(
        "image.base_url" -> "images/",
        "confluent.version" -> confluentAvroSerializerVersion,
        "scalatest.version" -> scalatestVersion,
        "scaladoc.akka.kafka.base_url" -> s"/${(Preprocess / siteSubdirName).value}/",
        "javadoc.akka.kafka.base_url" -> "",
        // Akka
        "akka.version" -> akkaVersion,
        "extref.akka.base_url" -> s"https://doc.akka.io/docs/akka/$AkkaBinaryVersionForDocs/%s",
        "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/$AkkaBinaryVersionForDocs/",
        "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka/$AkkaBinaryVersionForDocs/",
        "javadoc.akka.link_style" -> "direct",
        "extref.akka-management.base_url" -> s"https://doc.akka.io/docs/akka-management/current/%s",
        // Kafka
        "kafka.version" -> kafkaVersion,
        "extref.kafka.base_url" -> s"https://kafka.apache.org/$KafkaVersionForDocs/%s",
        "javadoc.org.apache.kafka.base_url" -> s"https://kafka.apache.org/$KafkaVersionForDocs/javadoc/",
        "javadoc.org.apache.kafka.link_style" -> "direct",
        // Java
        "extref.java-docs.base_url" -> "https://docs.oracle.com/en/java/javase/11/%s",
        "javadoc.base_url" -> "https://docs.oracle.com/en/java/javase/11/docs/api/java.base/",
        "javadoc.link_style" -> "direct",
        // Scala
        "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/current/",
        "scaladoc.com.typesafe.config.base_url" -> s"https://lightbend.github.io/config/latest/api/",
        // Testcontainers
        "testcontainers.version" -> testcontainersVersion,
        "javadoc.org.testcontainers.containers.base_url" -> s"https://www.javadoc.io/doc/org.testcontainers/testcontainers/$testcontainersVersion/",
        "javadoc.org.testcontainers.containers.link_style" -> "direct"
      ),
    apidocRootPackage := "akka",
    paradoxRoots := List("index.html"),
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifacts += makeSite.value -> "www/",
    publishRsyncHost := "akkarepo@gustav.akka.io"
  )

lazy val benchmarks = project
  .dependsOn(core, testkit)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin, SitePlugin)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(Defaults.itSettings)
  .settings(headerSettings(IntegrationTest))
  .settings(
    name := "akka-stream-kafka-benchmarks",
    publish / skip := true,
    IntegrationTest / parallelExecution := false,
    libraryDependencies ++= Seq(
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
        "io.dropwizard.metrics" % "metrics-core" % "4.2.11",
        "ch.qos.logback" % "logback-classic" % "1.2.11",
        "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
        "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "3.0.4",
        "org.testcontainers" % "kafka" % testcontainersVersion % IntegrationTest,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % IntegrationTest,
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % IntegrationTest,
        "org.scalatest" %% "scalatest" % scalatestVersion % IntegrationTest
      )
  )
