/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.kafka.testkit.scaladsl.KafkaSpec
import akka.kafka.testkit.internal.TestFrameworkInterface
import akka.stream.scaladsl.Flow
import org.scalatest.Suite
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

abstract class DocsSpecBase(kafkaPort: Int)
    extends KafkaSpec(kafkaPort)
    with AnyFlatSpecLike
    with TestFrameworkInterface.Scalatest
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with Eventually {

  this: Suite =>

  protected def this() = this(kafkaPort = -1)

  def businessFlow[T]: Flow[T, T, NotUsed] = Flow[T]

}
