/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks.app

import akka.kafka.benchmarks.BuildInfo
import akka.kafka.benchmarks.PerfFixtureHelpers.FilledTopic

case class RunTestCommand(testName: String,
                          kafkaHost: String,
                          msgCount: Int,
                          msgSize: Int,
                          numberOfPartitions: Int = 1) {
  val filledTopic = FilledTopic(msgCount = msgCount, msgSize = msgSize, numberOfPartitions = numberOfPartitions)
}
