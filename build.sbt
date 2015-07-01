import SonatypeKeys._

import scalariform.formatter.preferences.{SpacesAroundMultiImports, CompactControlReadability, PreserveSpaceBeforeArguments, DoubleIndentClassDeclaration}

sonatypeSettings

name := "reactive-kafka"

version := "0.7.0-SNAPSHOT"

organization := "com.softwaremill"

startYear := Some(2014)

licenses := Seq("Apache License 2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

homepage := Some(url("https://github.com/kciesielski/reactive-kafka"))

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

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0-RC4",
  "org.apache.kafka" %% "kafka" % "0.8.2.1",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.11" % "test",
  "org.reactivestreams" % "reactive-streams-tck" % "1.0.0" % "test"
)

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
    <url>git@github.com:kciesielski/reactive-kafka.git</url>
    <connection>scm:git:git@github.com:kciesielski/reactive-kafka.git</connection>
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