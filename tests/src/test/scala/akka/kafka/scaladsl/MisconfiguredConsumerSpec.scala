/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class MisconfiguredConsumerSpec
    extends TestKit(ActorSystem())
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with Eventually {

  implicit val materializer: Materializer = ActorMaterializer()
  implicit val patience = PatienceConfig(2.seconds, 20.millis)

  def bootstrapServers = "nowhere:6666"

  "Failing consumer construction" must {
    "be signalled to the stream by single sources" in assertAllStagesStopped {
      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group")
      val result = Consumer
        .plainSource(consumerSettings, Subscriptions.topics("topic"))
        .runWith(Sink.head)

      result.failed.futureValue shouldBe a[org.apache.kafka.common.KafkaException]
    }

    "be signalled to the stream by partitioned sources" in assertAllStagesStopped {
      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group")
      val result = Consumer
        .plainPartitionedSource(consumerSettings, Subscriptions.topics("topic"))
        .runWith(Sink.head)

      result.failed.futureValue shouldBe a[org.apache.kafka.common.KafkaException]
    }
  }
}
