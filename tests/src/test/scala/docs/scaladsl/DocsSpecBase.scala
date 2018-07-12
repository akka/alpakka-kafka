/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.kafka.internal.TestFrameworkInterface
import akka.kafka.scaladsl.{EmbeddedKafkaLike, KafkaSpec}
import akka.kafka.test.Utils.StageStoppingTimeout
import akka.stream.scaladsl.Flow
import org.scalatest.{FlatSpecLike, Matchers, Suite}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._

abstract class DocsSpecBase(kafkaPort: Int)
    extends KafkaSpec(kafkaPort)
    with FlatSpecLike
    with TestFrameworkInterface.Scalatest
    with EmbeddedKafkaLike
    with Matchers
    with ScalaFutures
    with Eventually {

  this: Suite ⇒
  implicit val stageStoppingTimeout: StageStoppingTimeout = StageStoppingTimeout(15.seconds)

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(5, Seconds)), interval = scaled(Span(15, Millis)))

  def businessFlow[T]: Flow[T, T, NotUsed] = Flow[T]

}
