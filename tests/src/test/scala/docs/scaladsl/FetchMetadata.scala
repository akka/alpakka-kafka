/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import org.scalatest.TryValues
import org.scalatest.time.{Seconds, Span}

// #metadata
import akka.actor.ActorRef
import akka.kafka.{KafkaConsumerActor, Metadata}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future
import scala.concurrent.duration._

// #metadata

class FetchMetadata extends DocsSpecBase with TestcontainersKafkaLike with TryValues {

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(20, Seconds)), interval = scaled(Span(1, Seconds)))

  "Consumer metadata" should "be available" in {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic()
    // #metadata
    val timeout = 5.seconds
    val settings = consumerSettings.withMetadataRequestTimeout(timeout)
    implicit val askTimeout = Timeout(timeout)

    val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(settings))

    val topicsFuture: Future[Metadata.Topics] = (consumer ? Metadata.ListTopics).mapTo[Metadata.Topics]

    topicsFuture.map(_.response.foreach { map =>
      println("Found topics:")
      map.foreach {
        case (topic, partitionInfo) =>
          partitionInfo.foreach { info =>
            println(s"  $topic: $info")
          }
      }
    })
    // #metadata
    topicsFuture.futureValue.response should be a Symbol("success")
    topicsFuture.futureValue.response.get(topic) should not be Symbol("empty")
  }

  "Get offsets" should "timeout fast" in {
    val consumerSettings = consumerDefaults
      .withGroupId(createGroupId())
      .withMetadataRequestTimeout(100.millis)
    val topic = createTopic()
    implicit val timeout = Timeout(consumerSettings.metadataRequestTimeout * 2)

    val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))

    val nonExistentPartition = 42
    val topicsFuture: Future[Metadata.EndOffsets] =
      (consumer ? Metadata.GetEndOffsets(Set(new TopicPartition(topic, nonExistentPartition))))
        .mapTo[Metadata.EndOffsets]

    val response = topicsFuture.futureValue.response
    response should be a Symbol("failure")
    response.failed.get shouldBe a[org.apache.kafka.common.errors.TimeoutException]
  }

  it should "return" in {
    val consumerSettings = consumerDefaults
      .withGroupId(createGroupId())
      .withMetadataRequestTimeout(5.seconds)
    val topic = createTopic()
    implicit val timeout = Timeout(consumerSettings.metadataRequestTimeout * 2)

    val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))

    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val topicsFuture: Future[Metadata.EndOffsets] =
      (consumer ? Metadata.GetEndOffsets(Set(tp))).mapTo[Metadata.EndOffsets]

    val response = topicsFuture.futureValue.response
    response should be a Symbol("success")
    response.get(tp) should be(0L)
  }
}
