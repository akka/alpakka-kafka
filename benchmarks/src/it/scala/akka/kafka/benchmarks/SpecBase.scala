/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.kafka.testkit.scaladsl.ScalatestKafkaSpec
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

abstract class SpecBase(kafkaPort: Int)
    extends ScalatestKafkaSpec(kafkaPort)
    with AnyFlatSpecLike
    with Matchers
    with ScalaFutures
    with Eventually {

  protected def this() = this(kafkaPort = -1)
}
