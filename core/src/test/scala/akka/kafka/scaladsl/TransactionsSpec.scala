/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.Subscriptions.TopicSubscription
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.test.Utils._
import akka.stream.scaladsl.{Keep, RestartSource, Sink}
import akka.stream.testkit.scaladsl.TestSink
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Await
import scala.concurrent.duration._

class TransactionsSpec extends SpecBase(kafkaPort = KafkaPorts.TransactionsSpec) {

  def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort, zooKeeperPort,
      Map(
        "offsets.topic.replication.factor" -> "1"
      ))

  "Transactions" must {

    "complete a consume-transform-produce cycle" in {
      assertAllStagesStopped {
        val sourceTopic = createTopic(1)
        val sinkTopic = createTopic(2)
        val group = createGroup(1)

        givenInitializedTopic(sourceTopic)
        givenInitializedTopic(sinkTopic)

        Await.result(produce(sourceTopic, 1 to 100), remainingOrDefault)

        val consumerSettings = consumerDefaults.withGroupId(group)

        val control = Consumer.transactionalSource(consumerSettings, TopicSubscription(Set(sourceTopic), None))
          .filterNot(_.record.value() == InitialMsg)
          .map { msg =>
            ProducerMessage.Message(
              new ProducerRecord[String, String](sinkTopic, msg.record.value), msg.partitionOffset)
          }
          .via(Producer.transactionalFlow(producerDefaults, group))
          .toMat(Sink.ignore)(Keep.left)
          .run()

        val probeConsumerGroup = createGroup(2)
        val probeConsumerSettings = consumerDefaults.withGroupId(probeConsumerGroup)
          .withProperties(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")

        val probeConsumer = Consumer.plainSource(probeConsumerSettings, TopicSubscription(Set(sinkTopic), None))
          .filterNot(_.value == InitialMsg)
          .map(_.value())
          .runWith(TestSink.probe)

        probeConsumer
          .request(100)
          .expectNextN((1 to 100).map(_.toString))

        probeConsumer.cancel()
        Await.result(control.shutdown(), remainingOrDefault)
      }
    }

    "complete a consume-transform-produce transaction with transient failure causing an abort with restartable source" in {
      assertAllStagesStopped {
        val sourceTopic = createTopic(1)
        val sinkTopic = createTopic(2)
        val group = createGroup(1)

        givenInitializedTopic(sourceTopic)
        givenInitializedTopic(sinkTopic)

        Await.result(produce(sourceTopic, 1 to 1000), remainingOrDefault)

        val consumerSettings = consumerDefaults.withGroupId(group)

        var restartCount = 0
        var innerControl = null.asInstanceOf[Control]

        val restartSource = RestartSource.onFailuresWithBackoff(
          minBackoff = 0.1.seconds,
          maxBackoff = 1.seconds,
          randomFactor = 0.2
        ) { () =>
          restartCount += 1
          Consumer.transactionalSource(consumerSettings, TopicSubscription(Set(sourceTopic), None))
            .filterNot(_.record.value() == InitialMsg)
            .map { msg =>
              if (msg.record.value().toInt == 500 && restartCount < 2) {
                // add a delay that equals or exceeds EoS commit interval to trigger a commit for everything
                // up until this record (0 -> 500)
                Thread.sleep(producerDefaults.eosCommitInterval.toMillis + 10)
              }
              if (msg.record.value().toInt == 501 && restartCount < 2) {
                throw new RuntimeException("Uh oh..")
              }
              else {
                ProducerMessage.Message(
                  new ProducerRecord[String, String](sinkTopic, msg.record.value()), msg.partitionOffset)
              }
            }
            // side effect out the `Control` materialized value because it can't be propagated through the `RestartSource`
            .mapMaterializedValue(innerControl = _)
            .via(Producer.transactionalFlow(producerDefaults, group))
        }

        restartSource.runWith(Sink.ignore)

        val probeGroup = createGroup(2)
        val probeConsumerSettings = consumerDefaults.withGroupId(probeGroup)
          .withProperties(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")

        val probeConsumer = Consumer.plainSource(probeConsumerSettings, TopicSubscription(Set(sinkTopic), None))
          .filterNot(_.value == InitialMsg)
          .map(_.value())
          .runWith(TestSink.probe)

        probeConsumer
          .request(1000)
          .expectNextN((1 to 1000).map(_.toString))

        probeConsumer.cancel()
        Await.result(innerControl.shutdown(), remainingOrDefault)
      }
    }

  }
}
