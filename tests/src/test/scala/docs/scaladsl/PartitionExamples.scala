/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.kafka.scaladsl.Consumer
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike
import akka.kafka.{KafkaConsumerActor, Subscriptions}
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class PartitionExamples extends DocsSpecBase with TestcontainersKafkaPerClassLike {

  override val testcontainersSettings =
    KafkaTestkitTestcontainersSettings(system)
      .withInternalTopicsReplicationFactor(1)
      .withConfigureKafka { brokerContainers =>
        brokerContainers.foreach {
          _.withEnv("KAFKA_BROKER_ID", "1")
            .withEnv("KAFKA_NUM_PARTITIONS", "3")
            .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "3")
        }
      }

  "Externally controlled kafka consumer" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic(suffix = 0, partitions = 3)
    val partition1 = 1
    val partition2 = 2
    // #consumerActor
    //Consumer is represented by actor
    val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))

    //Manually assign topic partition to it
    val (controlPartition1, result1) = Consumer
      .plainExternalSource[String, Array[Byte]](
        consumer,
        Subscriptions.assignment(new TopicPartition(topic, partition1))
      )
      .via(businessFlow)
      .toMat(Sink.seq)(Keep.both)
      .run()

    //Manually assign another topic partition
    val (controlPartition2, result2) = Consumer
      .plainExternalSource[String, Array[Byte]](
        consumer,
        Subscriptions.assignment(new TopicPartition(topic, partition2))
      )
      .via(businessFlow)
      .toMat(Sink.seq)(Keep.both)
      .run()

    // ....

    // #consumerActor
    awaitProduce(produce(topic, 1 to 10, partition1), produce(topic, 1 to 10, partition2))
    awaitMultiple(2.seconds,
                  // #consumerActor
                  controlPartition1.shutdown()
                  // #consumerActor
                  ,
                  // #consumerActor
                  controlPartition2.shutdown()
                  // #consumerActor
    )
    // #consumerActor
    consumer ! KafkaConsumerActor.Stop
    // #consumerActor

    result1.futureValue should have size 10
    result2.futureValue should have size 10
  }

  "Typed Externally controlled kafka consumer" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())

    val _ = Behaviors.setup[Nothing] { context =>
      // #consumerActorTyped
      // adds support for actors to a classic actor system and context
      import akka.actor.typed.scaladsl.adapter._

      //Consumer is represented by actor
      // #consumerActorTyped
      @nowarn("cat=unused")
      // #consumerActorTyped
      val consumer: ActorRef =
        context.actorOf(KafkaConsumerActor.props(consumerSettings), "kafka-consumer-actor")
      // #consumerActorTyped

      Behaviors.empty
    }

  }

  "Consumer Metrics" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic(suffix = 0, partitions = 3)
    val partition = 1
    def println(s: String): Unit = {}
    // #consumerMetrics
    val control: Consumer.Control = Consumer
      .plainSource(consumerSettings, Subscriptions.assignment(new TopicPartition(topic, partition)))
      .via(businessFlow)
      .to(Sink.ignore)
      .run()

    // #consumerMetrics
    // can be removed when https://github.com/akka/alpakka-kafka/issues/528 is fixed
    sleep(500.millis)
    // #consumerMetrics

    val metrics: Future[Map[MetricName, Metric]] = control.metrics
    metrics.foreach(map => println(s"metrics: ${map.mkString("\n")}"))
    // #consumerMetrics
    Await.result(metrics, 4.seconds) should not be Symbol("empty")
    control.shutdown()
  }

}
