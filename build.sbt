import SonatypeKeys._

import scalariform.formatter.preferences.{SpacesAroundMultiImports, CompactControlReadability, PreserveSpaceBeforeArguments, DoubleIndentClassDeclaration}

sonatypeSettings

name := "reactive-kafka"

version := "0.7.1"

organization := "com.softwaremill"

startYear := Some(2014)

licenses := Seq("Apache License 2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

homepage := Some(url("https://github.com/softwaremill/reactive-kafka"))

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.10.5", "2.11.7")

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
)

val akkaVersion = "2.3.11"
val curatorVersion = "2.8.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0",
  "org.apache.kafka" %% "kafka" % "0.8.2.1" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.curator" % "curator-framework" % curatorVersion,
  "org.apache.curator" % "curator-recipes" % curatorVersion,
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.reactivestreams" % "reactive-streams-tck" % "1.0.0" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "junit" % "junit" % "4.12" % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test",
  "org.mockito" % "mockito-core" % "1.10.19" % "test"
)

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")

publishMavenStyle := true
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
pomIncludeRepository := {
  x => false
}
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
  )

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(PreserveSpaceBeforeArguments, true)
  .setPreference(CompactControlReadability, true)
  .setPreference(SpacesAroundMultiImports, false)
