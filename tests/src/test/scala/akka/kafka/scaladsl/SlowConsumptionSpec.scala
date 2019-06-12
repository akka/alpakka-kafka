/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.testkit.scaladsl.EmbeddedKafkaLike
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._

import scala.concurrent.Future
import scala.concurrent.duration._
import collection.JavaConverters._

class SlowConsumptionSpec extends SpecBase(kafkaPort = KafkaPorts.SlowConsumptionSpec) with EmbeddedKafkaLike {

  override def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort,
                        zooKeeperPort,
                        Map(
                          "offsets.topic.replication.factor" -> "1",
                          "offsets.retention.minutes" -> "1",
                          "offsets.retention.check.interval.ms" -> "100"
                        ))

  implicit val patience: PatienceConfig = PatienceConfig(30 seconds, 2 second)

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
      val topic1 = createTopic(1, 3)
      val group1 = createGroupId(1)

      val producerSettings = ProducerSettings(system, new StringSerializer, new IntegerSerializer)
        .withBootstrapServers(bootstrapServers)
        .withProperty("batch.size", "50") // max batches of 50 bytes to trigger more fetch requests on the consumer side
      val producer = producerSettings.createKafkaProducer()
      val (producing, lastProduced) = Source
        .fromIterator(() => Iterator.from(2))
        .viaMat(KillSwitches.single)(Keep.right)
        .map(
          n => ProducerMessage.Message[String, Integer, Integer](new ProducerRecord(topic1, n), n)
        )
        .via(Producer.flexiFlow(producerSettings, producer))
        .map(_.passThrough)
        .toMat(Sink.last)(Keep.both)
        .run()

      val control: DrainingControl[Integer] = Consumer
        .plainSource(
          ConsumerSettings(system, new StringDeserializer, new IntegerDeserializer)
            .withBootstrapServers(bootstrapServers)
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withGroupId(group1)
            .withProperty("max.poll.records", "1"),
          Subscriptions.topics(topic1)
        )
        .throttle(100, 1.seconds)
        .map(_.value())
        .toMat(Sink.last)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      sleep(30.seconds)

      println("Producer metrics")
      producer
        .metrics()
        .asScala
        .filter {
          case (name, _) => List("records-per-request-avg").contains(name.name())
        }
        .foreach {
          case (name, metric) =>
            println(s"${name.name()} ${metric.metricValue()}")
        }

      producing.shutdown()

      println("Consumer metrics")
      control.metrics.futureValue
        .filter {
          case (name, _) =>
            List("bytes-consumed-total",
                 "fetch-rate",
                 "fetch-total",
                 "records-per-request-avg",
                 "records-consumed-total").contains(name.name())
        }
        // filter out topic-level or partition-level metrics
        .filterNot {
          case (name, _) => name.tags.containsKey("topic") || name.tags.containsKey("partition")
        }
        .foreach {
          case (name, metric) =>
            println(s"${name.name()} ${metric.metricValue()}")
        }

      control.drainAndShutdown().futureValue

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
      succeed
    }
  }
}
