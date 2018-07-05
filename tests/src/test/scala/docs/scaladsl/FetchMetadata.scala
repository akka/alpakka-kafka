/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

// #metadata
import akka.actor.ActorRef
import akka.kafka.{KafkaConsumerActor, KafkaPorts, Metadata}
import akka.pattern.ask
import akka.util.Timeout
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.scalatest.TryValues

import scala.concurrent.Future
import scala.concurrent.duration._

// #metadata

class FetchMetadata extends DocsSpecBase(KafkaPorts.ScalaFetchMetadataExamples) with TryValues {

  def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort, zooKeeperPort)

  "Consumer metadata" should "be available" in {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic()
    // #metadata
    implicit val timeout = Timeout(5.seconds)

    val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))

    // ... create source ...

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
    topicsFuture.futureValue.response should be a 'success
    topicsFuture.futureValue.response.get(topic) should not be 'empty
  }
}
