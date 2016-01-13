package com.softwaremill.react.kafka.benchmarks

import scala.util.Try

trait ReactiveKafkaPerfTest {
  def warmup(): Unit = ()

  def run(): Try[String]

  def name: String

  def elemCount: Long
}
