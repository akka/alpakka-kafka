/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka.benchmarks

import scala.util.Try

/**
 * Represents a single performance test.
 */
trait PerfTest {

  /**
   * A warmup that should be executed before each single test execution.
   */
  def warmup(): Unit = ()

  /**
   * Synchronous test body.
   */
  def run(): Try[String]

  /**
   * Test name (will be used in reports.
   */
  def name: String
}
