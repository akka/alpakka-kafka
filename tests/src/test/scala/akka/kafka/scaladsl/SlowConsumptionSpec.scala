/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentLinkedQueue

import akka.Done
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.ProducerMessage.Message
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.test.Utils._
import akka.pattern.ask
import akka.stream.KillSwitches
import akka.stream.SubstreamCancelStrategies.Drain
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import akka.util.Timeout
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import org.scalatest._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Success

class SlowConsumptionSpec extends SpecBase(kafkaPort = KafkaPorts.SlowConsumptionSpec) with Inside {

  def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort,
                        zooKeeperPort,
                        Map(
                          "offsets.topic.replication.factor" -> "1"
                        ))

  /* This test case exercises a slow consumer which leads to a behaviour where the Kafka client
   * discards already fetched records, which is shown by the `o.a.k.c.consumer.internals.Fetcher`
   * log message
   * "Not returning fetched records for assigned partition topic-1-1-0 since it is no longer fetchable".
   *
   * Add <logger name="org.apache.kafka.clients.consumer.internals.Fetcher" level="DEBUG"/>
   * to tests/src/test/resources/logback-test.xml to see it.
   *
   * It was implemented to understand the unwanted behaviour reported in issue #549
   * https://github.com/akka/alpakka-kafka/issues/549
   *
   * I did not find a way to inspect this discarding of fetched records from the outside, yet.
   */
  "Reading slower than writing" must {
    "work" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)

      val producerSettings = ProducerSettings(system, new StringSerializer, new IntegerSerializer)
        .withBootstrapServers(bootstrapServers)
      val (producing, lastProduced) = Source
        .fromIterator(() => Iterator.from(2))
        .viaMat(KillSwitches.single)(Keep.right)
        .map(
          n =>
            ProducerMessage.Message[String, Integer, Integer](new ProducerRecord(topic1, partition0, DefaultKey, n), n)
        )
        .via(Producer.flexiFlow(producerSettings))
        .map(_.passThrough)
        .toMat(Sink.last)(Keep.both)
        .run()

      val control = Consumer
        .plainSource(
          ConsumerSettings(system, new StringDeserializer, new IntegerDeserializer)
            .withBootstrapServers(bootstrapServers)
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withWakeupTimeout(10.seconds)
            .withMaxWakeups(10)
            .withGroupId(group1)
            .withProperty("max.poll.records", "5"),
          Subscriptions.topics(topic1)
        )
        .throttle(1, 1.seconds)
        .map(_.value())
        .toMat(Sink.last)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      sleep(5.seconds)
      producing.shutdown()
      val lastConsumed = control.drainAndShutdown()
      val (produced, consumed) = Future
        .sequence(Seq(lastProduced, lastConsumed))
        .map {
          case Seq(produced, consumed) =>
            println(s"last element produced $produced")
            println(s"last element consumed $consumed")
            (produced, consumed)
        }
        .futureValue
      produced should be > consumed
    }
  }
}
