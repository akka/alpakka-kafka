import scalariform.formatter.preferences.{CompactControlReadability, DoubleIndentClassDeclaration, PreserveSpaceBeforeArguments, SpacesAroundMultiImports}

name := "reactive-kafka"

val akkaVersion = "2.4.4"
val curatorVersion = "2.9.0"

val kafka = "org.apache.kafka" %% "kafka" % "0.8.2.2" exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j")
val curator = Seq("org.apache.curator" % "curator-framework" % curatorVersion,
  "org.apache.curator" % "curator-recipes" % curatorVersion
)

val commonDependencies = Seq(
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
)

val coreDependencies = Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  kafka,
  "org.slf4j" % "log4j-over-slf4j" % "1.7.12",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "org.reactivestreams" % "reactive-streams-tck" % "1.0.0" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "junit" % "junit" % "4.12" % "test",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test",
  "org.mockito" % "mockito-core" % "1.10.19" % "test"
)

val zkCommitterDependencies = Seq(kafka) ++ curator

val commonSettings =
sonatypeSettings ++ scalariformSettings ++ Seq(
  version := "0.8.7",
  organization := "com.softwaremill.reactivekafka",
  startYear := Some(2014),
  licenses := Seq("Apache License 2.0" -> url("http://opensource.org/licenses/Apache-2.0")),
  homepage := Some(url("https://github.com/softwaremill/reactive-kafka")),
  scalaVersion := "2.11.8",
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
  "-Ywarn-value-discard",
  "-Xfuture"
),
testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),
  ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(PreserveSpaceBeforeArguments, true)
  .setPreference(CompactControlReadability, true)
  .setPreference(SpacesAroundMultiImports, false))

val publishSettings = Seq(
  publishMavenStyle := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  pomIncludeRepository := {
    x => false
  },
  pomExtra := (
    <scm>
      <url>git@github.com:softwaremill/reactive-kafka.git</url>
      <connection>scm:git:git@github.com:softwaremill/reactive-kafka.git</connection>
    </scm>
      <developers>
        <developer>
          <id>kciesielski</id>
          <name>Krzysztof Ciesielski</name>
          <url>https://twitter.com/kpciesielski</url>
        </developer>
      </developers>
    ))

lazy val root =
  project.in( file(".") )
    .settings(commonSettings)
    .settings(Seq(
    publishArtifact := false,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))))
    .aggregate(zookeeperCommitter, core)

lazy val core = project
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(Seq(
    name := "reactive-kafka-core",
    libraryDependencies ++= commonDependencies ++ coreDependencies
))

lazy val zookeeperCommitter = Project(
id = "zookeeper-committer",
base = file("./zookeeper-committer")
)
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(libraryDependencies ++= commonDependencies ++ zkCommitterDependencies)
  .dependsOn(core % "compile->compile;test->test")
