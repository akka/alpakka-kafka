import SonatypeKeys._

sonatypeSettings

name := "reactive-kafka"

version := "0.1.0"

organization := "com.softwaremill"

startYear := Some(2014)

licenses := Seq("Apache License 2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

homepage := Some(url("https://github.com/kciesielski/reactive-kafka"))

scalaVersion := "2.10.4"

crossScalaVersions := Seq("2.10.4", "2.11.4")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings", "-target:jvm-1.7")

libraryDependencies ++= Seq(
  "org.scala-stm" %% "scala-stm" % "0.7",
  "com.typesafe" % "config" % "1.2.1",
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0-M2",
  "joda-time" % "joda-time" % "2.6",
  "org.joda" % "joda-convert" % "1.7",
  "com.google.guava" % "guava" % "18.0",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "com.typesafe.scala-logging" %% "scala-logging-api" % "2.1.2",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.slf4j" % "slf4j-api" % "1.6.6",
  "org.apache.kafka" %% "kafka" % "0.8.2-beta",
  "com.101tec" % "zkclient" % "0.3",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.7" % "test",
  "org.reactivestreams" % "reactive-streams-tck" % "1.0.0.RC1" % "test"
)
