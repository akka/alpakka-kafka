/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.ConcurrentLinkedQueue

import akka.Done
import akka.kafka._
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class RetentionPeriodSpec extends SpecBase with TestcontainersKafkaPerClassLike {

  override val testcontainersSettings = KafkaTestkitTestcontainersSettings(system)
  // The bug commit refreshing circumvents was fixed in Kafka 2.1.0
  // https://issues.apache.org/jira/browse/KAFKA-4682
  // Confluent Platform 5.0.0 bundles Kafka 2.0.0
  // https://docs.confluent.io/current/installation/versions-interoperability.html
    .withConfluentPlatformVersion("5.0.0")
    .withInternalTopicsReplicationFactor(1)
    .withConfigureKafka { brokerContainers =>
      brokerContainers.foreach {
        _.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
          .withEnv("KAFKA_OFFSETS_RETENTION_MINUTES", "1")
          .withEnv("KAFKA_OFFSETS_RETENTION_CHECK_INTERVAL_MS", "100")
      }
    }

  "After retention period (1 min) consumer" must {

    "resume from committed offset" in assertAllStagesStopped {
      val topic1 = createTopic()
      val group1 = createGroupId()

      produce(topic1, 1 to 100)

      val committedElements = new ConcurrentLinkedQueue[Int]()

      val consumerSettings = consumerDefaults.withGroupId(group1).withCommitRefreshInterval(5.seconds)

      val (control, probe1) = Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic1))
        .mapAsync(10) { elem =>
          elem.committableOffset.commitInternal().map { _ =>
            committedElements.add(elem.record.value.toInt)
            Done
          }
        }
        .toMat(TestSink.probe)(Keep.both)
        .run()

      probe1
        .request(25)
        .expectNextN(25)
        .toSet should be(Set(Done))

      val longerThanRetentionPeriod = 70.seconds
      sleep(longerThanRetentionPeriod, "Waiting for retention to expire for probe1")

      probe1.cancel()
      Await.result(control.isShutdown, remainingOrDefault)

      val probe2 = Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic1))
        .map(_.record.value)
        .runWith(TestSink.probe)

      // Note that due to buffers and mapAsync(10) the committed offset is more
      // than 26, and that is not wrong

      // some concurrent publish
      produce(topic1, 101 to 200)

      val expectedElements = ((committedElements.asScala.max + 1) to 100).map(_.toString)
      probe2
        .request(100)
        .expectNextN(expectedElements)

      sleep(longerThanRetentionPeriod, "Waiting for retention to expire for probe2")

      probe2.cancel()

      val probe3 = Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic1))
        .map(_.record.value)
        .runWith(TestSink.probe)

      probe3
        .request(100)
        .expectNextN(expectedElements)

      probe3.cancel()
    }
  }
}
