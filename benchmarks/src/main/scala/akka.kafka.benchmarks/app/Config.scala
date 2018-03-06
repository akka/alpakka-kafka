/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks.app

import com.typesafe.config.ConfigFactory

trait Config {
  private val config = ConfigFactory.load()
  protected val kafkaHost = config.getString("akka.kafka.benchmarks.bootstrap-server") + ":9092"
  protected val testName = config.getString("akka.kafka.benchmarks.test-name")
  protected val msgCount = config.getInt("akka.kafka.benchmarks.msg-count")

}
