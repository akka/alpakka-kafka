/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.Done
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.Future

class PartitionedSourcesSpec extends SpecBase(kafkaPort = KafkaPorts.PartitionedSourcesSpec) with Inside {

  implicit val patience = PatienceConfig(15.seconds, 500.millis)
  override def sleepAfterProduce: FiniteDuration = 500.millis

  def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort,
                        zooKeeperPort,
                        Map(
                          "offsets.topic.replication.factor" -> "1"
                        ))

  "Partitioned source" must {

    "begin consuming from the beginning of the topic" in assertAllStagesStopped {
      val topic = createTopic(1)
      val group = createGroupId(1)

      awaitProduce(produce(topic, 1 to 100))

      val probe = Consumer
        .plainPartitionedManualOffsetSource(consumerDefaults.withGroupId(group),
                                            Subscriptions.topics(topic),
                                            _ => Future.successful(Map.empty))
        .flatMapMerge(1, _._2)
        .map(_.value())
        .runWith(TestSink.probe)

      probe
        .request(100)
        .expectNextN((1 to 100).map(_.toString))

      probe.cancel()
    }

    "begin consuming from the middle of the topic" in assertAllStagesStopped {
      val topic = createTopic(1)
      val group = createGroupId(1)

      givenInitializedTopic(topic)

      awaitProduce(produce(topic, 1 to 100))

      val probe = Consumer
        .plainPartitionedManualOffsetSource(consumerDefaults.withGroupId(group),
                                            Subscriptions.topics(topic),
                                            tp => Future.successful(tp.map(_ -> 51L).toMap))
        .flatMapMerge(1, _._2)
        .filterNot(_.value == InitialMsg)
        .map(_.value())
        .runWith(TestSink.probe)

      probe
        .request(50)
        .expectNextN((51 to 100).map(_.toString))

      probe.cancel()
    }

    "get a source per partition" in assertAllStagesStopped {
      val partitions = 4
      val totalMessages = 400L

      val topic = createTopic(1, partitions)
      val allTps = (0 until partitions).map(p => new TopicPartition(topic, p))
      val group = createGroupId(1)
      val sourceSettings = consumerDefaults
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withGroupId(group)

      var createdSubSources = List.empty[TopicPartition]

      val control = Consumer
        .plainPartitionedSource(sourceSettings, Subscriptions.topics(topic))
        .groupBy(partitions, _._1)
        .mapAsync(8) {
          case (tp, source) =>
            log.info(s"Sub-source for $tp started")
            createdSubSources = tp :: createdSubSources
            source
              .scan(0L)((c, _) => c + 1)
              .runWith(Sink.last)
              .map { res =>
                log.info(s"Sub-source for $tp completed: Received [$res] messages in total.")
                res
              }
        }
        .mergeSubstreams
        .scan(0L)((c, subValue) => c + subValue)
        .toMat(Sink.last)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      sleep(10.seconds)

      val producer = Source(1L to totalMessages)
        .map(n => new ProducerRecord(topic, (n % partitions).toInt, DefaultKey, n.toString))
        .runWith(Producer.plainSink(producerDefaults, testProducer))

      producer.futureValue shouldBe Done
      sleep(5.seconds)
      createdSubSources should contain allElementsOf allTps
      val streamMessages = control.drainAndShutdown().futureValue
      streamMessages shouldBe totalMessages
    }

    "rebalance when a new consumer comes up" in assertAllStagesStopped {
      val partitions = 4
      val totalMessages = 400L

      val topic = createTopic(1, partitions)
      val allTps = (0 until partitions).map(p => new TopicPartition(topic, p))
      val group = createGroupId(1)
      val sourceSettings = consumerDefaults
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        .withGroupId(group)

      var createdSubSources = List.empty[TopicPartition]

      val control = Consumer
        .plainPartitionedSource(sourceSettings, Subscriptions.topics(topic))
        .groupBy(partitions, _._1)
        .mapAsync(8) {
          case (tp, source) =>
            createdSubSources = tp :: createdSubSources
            log.info(s"Sub-source for $tp")
            source
              .scan(0L)((c, _) => c + 1)
              .runWith(Sink.last)
              .map { res =>
                log.info(s"Sub-source for $tp completed: Received [$res] messages in total.")
                res
              }
        }
        .mergeSubstreams
        .scan(0L)((c, subValue) => c + subValue)
        .toMat(Sink.last)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      // make sure the Consumer starts before the producer
      sleep(10.seconds)

      var createdInnerSubSources = List.empty[TopicPartition]
      var control2: DrainingControl[Long] = null

      val producer = Source(1L to totalMessages)
        .map { number =>
          if (number == totalMessages / 2) {
            // create another consumer with the same groupId to trigger re-balancing
            control2 = Consumer
              .plainPartitionedSource(sourceSettings, Subscriptions.topics(topic))
              .groupBy(partitions, _._1)
              .mapAsync(8) {
                case (tp, source) =>
                  log.info(s"Inner Sub-source for $tp")
                  createdInnerSubSources = tp :: createdInnerSubSources
                  source
                    .scan(0L)((c, _) => c + 1)
                    .runWith(Sink.last)
                    .map { res =>
                      log.info(s"Inner Sub-source for $tp completed: Received [$res] messages in total.")
                      res
                    }
              }
              .mergeSubstreams
              .scan(0L)((c, subValue) => c + subValue)
              .toMat(Sink.last)(Keep.both)
              .mapMaterializedValue(DrainingControl.apply)
              .run()
          }
          number
        }
        .map(n => new ProducerRecord(topic, (n % partitions).toInt, DefaultKey, n.toString))
        .runWith(Producer.plainSink(producerDefaults, testProducer))

      producer.futureValue shouldBe Done
      sleep(5.seconds)
      eventually {
        control2 should not be null
      }
      createdSubSources should contain allElementsOf allTps
      createdInnerSubSources should have size 2
      val stream1messages = control.drainAndShutdown().futureValue
      val stream2messages = control2.drainAndShutdown().futureValue
      stream1messages + stream2messages shouldBe totalMessages
    }

    "call the onRevoked hook" in assertAllStagesStopped {
      val topic = createTopic()
      val group = createGroupId(1)

      awaitProduce(produce(topic, 1 to 100))

      var revoked = false

      val source = Consumer
        .plainPartitionedManualOffsetSource(consumerDefaults.withGroupId(group),
                                            Subscriptions.topics(topic),
                                            _ => Future.successful(Map.empty),
                                            _ => revoked = true)
        .flatMapMerge(1, _._2)
        .map(_.value())

      val (control1, probe1) = source.toMat(TestSink.probe)(Keep.both).run()

      probe1.request(50)

      sleep(consumerDefaults.waitClosePartition)

      val probe2 = source.runWith(TestSink.probe)

      eventually {
        assert(revoked, "revoked hook should have been called")
      }

      probe1.cancel()
      probe2.cancel()
      control1.isShutdown.futureValue should be(Done)
    }

  }
}
