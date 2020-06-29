import com.typesafe.tools.mima.core.{Problem, ProblemFilters}
import Dependencies.Versions._
enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-kafka"

resolvers in ThisBuild ++= Seq(
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
                          "https://gitter.im/akka/dev",
                          url("https://github.com/akka/alpakka-kafka/graphs/contributors")),
  startYear := Some(2014),
  licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")),
  description := "Alpakka is a Reactive Enterprise Integration library for Java and Scala, based on Reactive Streams and Akka.",
  crossScalaVersions := ScalaVersions,
  scalaVersion := Scala212,
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
      if (scalaBinaryVersion.value == "2.13") Seq.empty
      else Seq("-Yno-adapted-args", "-Xfuture")
    } ++ {
      if (insideCI.value && !BuildType.Nightly) Seq("-Xfatal-warnings")
      else Seq.empty
    },
  Compile / doc / scalacOptions := scalacOptions.value ++ Seq(
      "-doc-title",
      "Alpakka Kafka",
      "-doc-version",
      version.value,
      "-sourcepath",
      (baseDirectory in ThisBuild).value.toString,
      "-skip-packages",
      "akka.pattern:scala" // for some reason Scaladoc creates this
    ) ++ {
      scalaBinaryVersion.value match {
        case "2.12" | "2.13" =>
          Seq(
            "-doc-source-url", {
              val branch = if (isSnapshot.value) "master" else s"v${version.value}"
              s"https://github.com/akka/alpakka-kafka/tree/${branch}€{FILE_PATH_EXT}#L€{FILE_LINE}"
            },
            "-doc-canonical-base-url",
            "https://doc.akka.io/api/alpakka-kafka/current/"
          )
        case "2.11" =>
          Seq(
            "-doc-source-url", {
              val branch = if (isSnapshot.value) "master" else s"v${version.value}"
              s"https://github.com/akka/alpakka-kafka/tree/${branch}€{FILE_PATH}.scala#L1"
            }
          )
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
  scalafmtOnCompile := true,
  ThisBuild / mimaReportSignatureProblems := true,
  headerLicense := Some(
      HeaderLicense.Custom(
        """|Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
           |Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
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
    .disablePlugins(SitePlugin, MimaPlugin)
    .settings(commonSettings)
    .settings(
      skip in publish := true,
      // TODO: add clusterSharding to unidocProjectFilter when we drop support for Akka 2.5
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
            |  verifyCodeStyle
            |    checks if all of the code is formatted according to the configuration
            |
            |  verifyDocs
            |    builds all of the docs
            |
            |  test
            |    runs all the tests
            |
            |  tests/it:test
            |    run integration tests backed by Docker containers
            |
            |  tests/testOnly -- -t "A consume-transform-produce cycle must complete in happy-path scenario"
            |    run a single test with an exact name (use -z for partial match)
            |
            |  benchmarks/it:testOnly *.AlpakkaKafkaPlainConsumer
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
    crossScalaVersions := CustomScalaVersions,
    Dependencies.core,
    Compile / compile / scalacOptions += "-P:silencer:globalFilters=[import scala.collection.compat._]",
    mimaPreviousArtifacts := Set(
        organization.value %% name.value % previousStableVersion.value
          .getOrElse(throw new Error("Unable to determine previous version"))
      ),
    mimaBinaryIssueFilters += ProblemFilters.exclude[Problem]("akka.kafka.internal.*")
  )

lazy val testkit = project
  .dependsOn(core)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin, SitePlugin)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka-testkit",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka.testkit"),
    crossScalaVersions := CustomScalaVersions,
    JupiterKeys.junitJupiterVersion := "5.5.2",
    Dependencies.testkit,
    mimaPreviousArtifacts := Set(
        organization.value %% name.value % previousStableVersion.value
          .getOrElse(throw new Error("Unable to determine previous version"))
      )
  )

/**
 * TODO: Once Akka 2.5 is dropped:
 * - add to `alpakka-kafka` aggregate project
 * - move `ClusterShardingExample` to `tests` project
 * - remove all akka26 paradox properties
 */
lazy val clusterSharding = project
  .in(file("./cluster-sharding"))
  .dependsOn(core)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin, SitePlugin) // TODO: re-enable MiMa plugin after first release
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka-cluster-sharding",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka.cluster.sharding"),
    Dependencies.clusterSharding,
    mimaPreviousArtifacts := Set(
        organization.value %% name.value % previousStableVersion.value
          .getOrElse(throw new Error("Unable to determine previous version"))
      )
  )

lazy val tests = project
  .dependsOn(core, testkit)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin, SitePlugin)
  .configs(IntegrationTest.extend(Test))
  .settings(commonSettings)
  .settings(Defaults.itSettings)
  .settings(headerSettings(IntegrationTest))
  .settings(
    name := "akka-stream-kafka-tests",
    crossScalaVersions := CustomScalaVersions,
    Dependencies.tests,
    resolvers += "Confluent Maven Repo" at "https://packages.confluent.io/maven/",
    publish / skip := true,
    whitesourceIgnore := true,
    Test / fork := true,
    Test / parallelExecution := false,
    IntegrationTest / parallelExecution := false,
    Test / unmanagedSources / excludeFilter := {
      scalaBinaryVersion.value match {
        case "2.11" | "2.12" =>
          HiddenFileFilter
        case "2.13" =>
          HiddenFileFilter ||
          // TODO: Remove ignore once `"io.github.embeddedkafka" %% "embedded-kafka-schema-registry"` is released for Scala 2.13
          // https://github.com/embeddedkafka/embedded-kafka-schema-registry/issues/78
          "SerializationTest.java" ||
          "SerializationSpec.scala" ||
          "EmbeddedKafkaWithSchemaRegistryTest.java"
      }
    }
  )

lazy val docs = project
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(BintrayPlugin, MimaPlugin)
  .settings(commonSettings)
  .settings(
    name := "Alpakka Kafka",
    crossScalaVersions := CustomScalaVersions,
    publish / skip := true,
    whitesourceIgnore := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/alpakka-kafka/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Preprocess / preprocessRules := Seq(
        ("\\.java\\.scala".r, _ => ".java"),
        ("https://javadoc\\.io/page/".r, _ => "https://javadoc\\.io/static/"),
        // Add Java module name https://github.com/ThoughtWorksInc/sbt-api-mappings/issues/58
        ("https://docs\\.oracle\\.com/en/java/javase/11/docs/api/".r,
         _ => "https://docs\\.oracle\\.com/en/java/javase/11/docs/api/java.base/")
      ),
    Paradox / siteSubdirName := s"docs/alpakka-kafka/${projectInfoVersion.value}",
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxProperties ++= Map(
        "embeddedKafka.version" -> embeddedKafkaVersion,
        "confluent.version" -> confluentAvroSerializerVersion,
        "scalatest.version" -> scalatestVersion,
        "scaladoc.akka.kafka.base_url" -> s"/${(Preprocess / siteSubdirName).value}/",
        "javadoc.akka.kafka.base_url" -> "",
        // Akka
        "akka.version" -> akkaVersion,
        "extref.akka.base_url" -> s"https://doc.akka.io/docs/akka/$AkkaBinaryVersion/%s",
        "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/$AkkaBinaryVersion/",
        "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka/$AkkaBinaryVersion/",
        "javadoc.akka.link_style" -> "direct",
        "extref.akka-management.base_url" -> s"https://doc.akka.io/docs/akka-management/current/%s",
        // Akka 2.6. These can be removed when we drop Akka 2.5 support.
        "akka.version26" -> akkaVersion26,
        "extref.akka26.base_url" -> s"https://doc.akka.io/docs/akka/$AkkaBinaryVersion26/%s",
        "scaladoc.akka.actor.typed.base_url" -> s"https://doc.akka.io/api/akka/$AkkaBinaryVersion26/",
        "extref.akka.actor.typed.base_url" -> s"https://doc.akka.io/docs/akka/$AkkaBinaryVersion26/%s",
        "scaladoc.akka.cluster.sharding.typed.base_url" -> s"https://doc.akka.io/api/akka/$AkkaBinaryVersion26/",
        "extref.akka.cluster.sharding.typed.base_url" -> s"https://doc.akka.io/docs/akka/$AkkaBinaryVersion26/%s",
        // Kafka
        "kafka.version" -> kafkaVersion,
        "extref.kafka.base_url" -> s"https://kafka.apache.org/$kafkaVersionForDocs/%s",
        "javadoc.org.apache.kafka.base_url" -> s"https://kafka.apache.org/$kafkaVersionForDocs/javadoc/",
        "javadoc.org.apache.kafka.link_style" -> "frames",
        // Java
        "extref.java-docs.base_url" -> "https://docs.oracle.com/en/java/javase/11/%s",
        "javadoc.base_url" -> "https://docs.oracle.com/en/java/javase/11/docs/api/java.base/",
        "javadoc.link_style" -> "direct",
        // Scala
        "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/current/",
        "scaladoc.com.typesafe.config.base_url" -> s"https://lightbend.github.io/config/latest/api/",
        // Testcontainers
        "testcontainers.version" -> testcontainersVersion
      ),
    apidocRootPackage := "akka",
    paradoxRoots := List("index.html",
                         "release-notes/1.0-M1.html",
                         "release-notes/1.0-RC1.html",
                         "release-notes/1.0-RC2.html"),
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
    crossScalaVersions := CustomScalaVersions,
    publish / skip := true,
    whitesourceIgnore := true,
    IntegrationTest / parallelExecution := false,
    Dependencies.benchmarks
  )
