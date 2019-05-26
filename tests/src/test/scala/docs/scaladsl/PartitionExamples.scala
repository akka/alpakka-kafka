/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorRef
import akka.kafka.scaladsl.Consumer
import akka.kafka.testkit.scaladsl.EmbeddedKafkaLike
import akka.kafka.{KafkaConsumerActor, KafkaPorts, Subscriptions}
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class PartitionExamples extends DocsSpecBase(KafkaPorts.ScalaPartitionExamples) with EmbeddedKafkaLike {

  override def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort,
                        zooKeeperPort,
                        Map(
                          "broker.id" -> "1",
                          "num.partitions" -> "3",
                          "offsets.topic.replication.factor" -> "1",
                          "offsets.topic.num.partitions" -> "3"
                        ))

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
