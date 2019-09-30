/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class MisconfiguredProducerSpec
    extends TestKit(ActorSystem())
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with Eventually {

  implicit val materializer: Materializer = ActorMaterializer()
  implicit val patience = PatienceConfig(2.seconds, 20.millis)

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
