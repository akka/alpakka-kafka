/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka._
import akka.kafka.testkit.scaladsl.ScalatestKafkaSpec
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class NoBrokerSpec
    extends ScalatestKafkaSpec(KafkaPorts.NoBrokerSpec)
    with WordSpecLike
    with Matchers
    with ScalaFutures {

  override def bootstrapServers = s"localhost:$kafkaPort"

  "A Consumer" must {

    "fail if Kafka broker does not exist" in assertAllStagesStopped {
      val topic1 = createTopicName(1)
      val group1 = createGroupId(1)

      // produce messages
      // create a consumer
      val (_, probe) = createProbe(consumerDefaults
                                     .withGroupId(group1)
                                     .withWakeupTimeout(500.millis)
                                     .withMaxWakeups(1),
                                   topic1)

      probe.request(1)
      val failed = probe.expectError()
      failed shouldBe a[InitialPollFailed]
      failed.getMessage should startWith("Initial consumer poll")
    }
  }
}
