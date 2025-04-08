import com.typesafe.tools.mima.core.{Problem, ProblemFilters}
import com.geirsson.CiReleasePlugin

enablePlugins(AutomateHeaderPlugin)
disablePlugins(CiReleasePlugin)

name := "akka-stream-kafka"

val Nightly = sys.env.get("EVENT_NAME").contains("schedule")

// align ignore-prefixes in scripts/link-validator.conf
// align in release.yml
val Scala213 = "2.13.14"
val Scala3 = "3.3.4"
val ScalaVersions = Seq(Scala213, Scala3)

val Scala3Settings = Seq(crossScalaVersions := ScalaVersions)

val akkaVersion = "2.10.0"
val AkkaBinaryVersionForDocs = VersionNumber(akkaVersion).numbers match {
  case Seq(major, minor, _*) => s"$major.$minor"
}

// Keep .scala-steward.conf pin in sync
val kafkaVersion = "3.7.1"
val KafkaVersionForDocs = "37"
// This should align with the ScalaTest version used in the Akka testkit
// https://github.com/akka/akka/blob/main/project/Dependencies.scala#L44
val scalatestVersion = "3.2.16"
val testcontainersVersion = "1.20.4"
val slf4jVersion = "2.0.16"
// this depends on Kafka, and should be upgraded to such latest version
// that depends on the same Kafka version, as is defined above
// See https://mvnrepository.com/artifact/io.confluent/kafka-avro-serializer?repo=confluent-packages
val confluentAvroSerializerVersion = "7.4.7"
val confluentLibsExclusionRules = Seq(
  ExclusionRule("log4j", "log4j"),
  ExclusionRule("org.slf4j", "slf4j-log4j12"),
  ExclusionRule("com.typesafe.scala-logging"),
  ExclusionRule("org.apache.kafka")
)

ThisBuild / resolvers ++= Seq(
  "Akka library repository".at("https://repo.akka.io/maven"),
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
addCommandAlias("verifyDocs", ";doc ;unidoc ;docs/paradoxBrowse")

// Java Platform version for JavaDoc creation
// sync with Java version in .github/workflows/release.yml#documentation
lazy val JavaDocLinkVersion = 17

val commonSettings = Def.settings(
  organization := "com.typesafe.akka",
  organizationName := "Lightbend Inc.",
  organizationHomepage := Some(url("https://www.lightbend.com/")),
  homepage := Some(url("https://doc.akka.io/libraries/alpakka-kafka/current")),
  scmInfo := Some(ScmInfo(url("https://github.com/akka/alpakka-kafka"), "git@github.com:akka/alpakka-kafka.git")),
  developers += Developer("contributors",
                          "Contributors",
                          "",
                          url("https://github.com/akka/alpakka-kafka/graphs/contributors")),
  startYear := Some(2014),
  releaseNotesURL := (
      if ((ThisBuild / isSnapshot).value) None
      else Some(url(s"https://github.com/akka/alpakka-kafka/releases/tag/v${version.value}"))
    ),
  licenses := {
    val tagOrBranch =
      if (version.value.endsWith("SNAPSHOT")) "main"
      else "v" + version.value
    Seq(("BUSL-1.1", url(s"https://raw.githubusercontent.com/akka/alpakka-kafka/${tagOrBranch}/LICENSE")))
  },
  description := "Alpakka is a Reactive Enterprise Integration library for Java and Scala, based on Reactive Streams and Akka.",
  crossScalaVersions := ScalaVersions,
  scalaVersion := Scala213,
  crossVersion := CrossVersion.binary,
  // append -SNAPSHOT to version when isSnapshot
  ThisBuild / dynverSonatypeSnapshots := true,
  javacOptions ++= Seq(
      "-Xlint:deprecation",
      "-Xlint:unchecked",
      "--release",
      "11"
    ),
  scalacOptions ++= Seq(
      "-encoding",
      "UTF-8", // yes, this is 2 args
      "-release",
      "11"
    ) ++ {
      if (scalaVersion.value != Scala3) Seq("-Werror")
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
      "-doc-source-url", {
        val branch = if (isSnapshot.value) "main" else s"v${version.value}"
        s"https://github.com/akka/alpakka-kafka/tree/${branch}€{FILE_PATH_EXT}#L€{FILE_LINE}"
      },
      "-doc-canonical-base-url",
      "https://doc.akka.io/api/alpakka-kafka/current/"
    ) ++ {
      if (scalaBinaryVersion.value.startsWith("3")) {
        Seq(s"-external-mappings:https://docs.oracle.com/en/java/javase/${JavaDocLinkVersion}/docs/api/java.base/",
            "-skip-packages:akka.pattern")
      } else {
        Seq("-jdk-api-doc-base",
            s"https://docs.oracle.com/en/java/javase/${JavaDocLinkVersion}/docs/api/java.base/",
            "-skip-packages",
            "akka.pattern")
      }
    },
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
           |Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
           |""".stripMargin
      )
    ),
  projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value),
  // can be removed once we this project is on 2.13.15
  dependencyOverrides += "org.scala-lang" % "scala-library" % Scala213
)

lazy val `alpakka-kafka` =
  project
    .in(file("."))
    .enablePlugins(ScalaUnidocPlugin)
    .disablePlugins(SitePlugin, MimaPlugin, CiReleasePlugin)
    .settings(commonSettings)
    .settings(
      crossScalaVersions := Nil,
      publish / skip := true,
      ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(core, testkit, clusterSharding),
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
            |  docs - the sources for generating https://doc.akka.io/libraries/alpakka-kafka/current
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
  .disablePlugins(SitePlugin, CiReleasePlugin)
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
  .settings(Scala3Settings)

lazy val testkit = project
  .dependsOn(core)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(SitePlugin, CiReleasePlugin)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka-testkit",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka.testkit"),
    JupiterKeys.junitJupiterVersion := "5.10.4",
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
  .settings(Scala3Settings)

lazy val clusterSharding = project
  .in(file("./cluster-sharding"))
  .dependsOn(core)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(SitePlugin, CiReleasePlugin)
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
  .configs(IntegrationTest) // make CI not fail
  .settings(Scala3Settings)

lazy val tests = project
  .dependsOn(core, testkit, clusterSharding)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin, SitePlugin, CiReleasePlugin)
  .configs(IntegrationTest.extend(Test))
  .settings(commonSettings)
  .settings(Defaults.itSettings)
  .settings(headerSettings(IntegrationTest))
  .settings(
    name := "akka-stream-kafka-tests",
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
        "com.google.protobuf" % "protobuf-java" % "3.25.5", // use the same, or later, version as in scalapb
        "io.confluent" % "kafka-avro-serializer" % confluentAvroSerializerVersion % Test excludeAll (confluentLibsExclusionRules: _*),
        // See https://github.com/sbt/sbt/issues/3618#issuecomment-448951808
        "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts Artifact("javax.ws.rs-api", "jar", "jar"),
        "org.testcontainers" % "kafka" % testcontainersVersion % Test,
        "org.scalatest" %% "scalatest" % scalatestVersion % Test,
        "io.spray" %% "spray-json" % "1.3.6" % Test,
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.2" % Test, // ApacheV2
        "org.junit.vintage" % "junit-vintage-engine" % JupiterKeys.junitVintageVersion.value % Test,
        // See http://hamcrest.org/JavaHamcrest/distributables#upgrading-from-hamcrest-1x
        "org.hamcrest" % "hamcrest-library" % "3.0" % Test,
        "org.hamcrest" % "hamcrest" % "3.0" % Test,
        "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Test,
        "ch.qos.logback" % "logback-classic" % "1.5.18" % Test,
        "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % Test,
        // Schema registry uses Glassfish which uses java.util.logging
        "org.slf4j" % "jul-to-slf4j" % slf4jVersion % Test,
        "org.mockito" % "mockito-core" % "5.14.0" % Test,
        "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.17" % Test
      ),
    resolvers ++= Seq(
        "Confluent Maven Repo" at "https://packages.confluent.io/maven/"
      ),
    publish / skip := true,
    Test / fork := true,
    Test / parallelExecution := false,
    IntegrationTest / parallelExecution := false,
    Test / unmanagedSources :=
      // Workaround for Scala 3 cross-java-compile issue (disabling compilation of Java tests on Scala 3 for now)
      // https://github.com/scala/scala3/issues/20026
      (if (scalaVersion.value.startsWith("3"))
         (Test / unmanagedSources).value.filterNot(_.name == "TestkitTestcontainersTest.java")
       else (Test / unmanagedSources).value)
  )

lazy val docs = project
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, SitePreviewPlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(MimaPlugin, CiReleasePlugin)
  .settings(commonSettings)
  .settings(
    name := "Alpakka Kafka",
    publish / skip := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/alpakka-kafka/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Preprocess / preprocessRules := Seq(
        ("https://javadoc\\.io/page/".r, _ => "https://javadoc\\.io/static/")
      ),
    Paradox / siteSubdirName := s"libraries/alpakka-kafka/${projectInfoVersion.value}",
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxProperties ++= Map(
        "image.base_url" -> "images/",
        "confluent.version" -> confluentAvroSerializerVersion,
        "scalatest.version" -> scalatestVersion,
        "scaladoc.akka.kafka.base_url" -> s"/${(Preprocess / siteSubdirName).value}/",
        "javadoc.akka.kafka.base_url" -> "",
        // Akka
        "akka.version" -> akkaVersion,
        "extref.akka.base_url" -> s"https://doc.akka.io/libraries/akka-core/$AkkaBinaryVersionForDocs/%s",
        "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/$AkkaBinaryVersionForDocs/",
        "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka/$AkkaBinaryVersionForDocs/",
        "javadoc.akka.link_style" -> "direct",
        "extref.akka-management.base_url" -> s"https://doc.akka.io/libraries/akka-management/current/%s",
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
  .disablePlugins(MimaPlugin, SitePlugin, CiReleasePlugin)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(Defaults.itSettings)
  .settings(headerSettings(IntegrationTest))
  .settings(
    name := "akka-stream-kafka-benchmarks",
    publish / skip := true,
    IntegrationTest / parallelExecution := false,
    libraryDependencies ++= Seq(
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
        "io.dropwizard.metrics" % "metrics-core" % "4.2.27",
        "ch.qos.logback" % "logback-classic" % "1.2.13",
        "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
        // FIXME akka-stream-alpakka-csv removed for now, because of dependency cycle
        // "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "4.0.0",
        "org.testcontainers" % "kafka" % testcontainersVersion % IntegrationTest,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % IntegrationTest,
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % IntegrationTest,
        "org.scalatest" %% "scalatest" % scalatestVersion % IntegrationTest
      )
  )
