/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.tests.scaladsl.LogCapturing
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class MisconfiguredProducerSpec
    extends TestKit(ActorSystem())
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with Eventually
    with IntegrationPatience
    with LogCapturing {

  "Failing producer construction" must {
    "fail stream appropriately" in assertAllStagesStopped {
      val producerSettings =
        ProducerSettings(system, new StringSerializer, new StringSerializer)
          .withBootstrapServers("invalid-bootstrap-server")

      val completion = Source
        .single(new ProducerRecord[String, String]("topic", "key", "value"))
        .runWith(Producer.plainSink(producerSettings))

      val exception = completion.failed.futureValue
      exception shouldBe a[org.apache.kafka.common.KafkaException]
      exception.getMessage shouldBe "Failed to construct kafka producer"
    }
  }
}
