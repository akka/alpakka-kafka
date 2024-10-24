/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.kafka.benchmarks.app.RunTestCommand

case class FixtureGen[F](command: RunTestCommand, generate: Int => F)
