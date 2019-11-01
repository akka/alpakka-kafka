/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.ConcurrentLinkedQueue

import akka.Done
import akka.kafka._
import akka.kafka.testkit.scaladsl.EmbeddedKafkaLike
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import net.manub.embeddedkafka.EmbeddedKafkaConfig

import scala.jdk.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class RetentionPeriodSpec extends SpecBase(kafkaPort = KafkaPorts.RetentionPeriodSpec) with EmbeddedKafkaLike {

  override def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort,
                        zooKeeperPort,
                        Map(
                          "offsets.topic.replication.factor" -> "1",
                          "offsets.retention.minutes" -> "1",
                          "offsets.retention.check.interval.ms" -> "100"
                        ))

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

      val longerThanRetentionPeriod = 70000L
      Thread.sleep(longerThanRetentionPeriod)

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

      Thread.sleep(longerThanRetentionPeriod)

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
