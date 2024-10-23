/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.Done
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

class CommittableSinkSpec extends SpecBase with TestcontainersKafkaLike {

  final val Numbers = (1 to 200).map(_.toString)
  final val partition1 = 1

  "Consumer to producer" must {

    "commit after producing to the target topic" in assertAllStagesStopped {
      val Messages = Numbers
      val topic1 = createTopic(1)
      val targetTopic = createTopic(2)
      val group1 = createGroupId(1)

      produceString(topic1, Messages)

      val consumerSettings = consumerDefaults.withGroupId(group1)

      val copying = Consumer
        .sourceWithOffsetContext(consumerSettings, Subscriptions.topics(topic1))
        .map { record =>
          ProducerMessage.single(new ProducerRecord(targetTopic, record.key(), record.value()))
        }
        .toMat(Producer.committableSinkWithOffsetContext(producerDefaults, committerDefaults))(DrainingControl.apply)
        .run()

      // read copied messages
      val (_, targetConsumer) = createProbe(consumerSettings, targetTopic)
      val copied = targetConsumer
        .request(Messages.size.toLong)
        .expectNextN(Messages.size.toLong)
      copied should contain theSameElementsInOrderAs Messages
      targetConsumer.cancel()

      copying.drainAndShutdown().futureValue shouldBe Done

      // other consumer in the same group should not receive any
      val (_, sourceConsumer) = createProbe(consumerSettings, topic1)
      sourceConsumer.request(10).expectNoMessage(5.seconds)
      sourceConsumer.cancel()
    }

    "with partitioned source copy all elements" in assertAllStagesStopped {
      val Messages = Numbers
      val partitions = 10
      val topic1 = createTopic(1, partitions)
      val targetTopic = createTopic(2)
      val group1 = createGroupId(1)

      produceStringRoundRobin(topic1, Messages)

      val consumerSettings = consumerDefaults.withGroupId(group1)

      val copying = Consumer
        .committablePartitionedSource(consumerSettings, Subscriptions.topics(topic1))
        .mapAsyncUnordered(parallelism = partitions) {
          case (_, source) =>
            source
              .map { message =>
                ProducerMessage.single(new ProducerRecord(targetTopic, message.record.key(), message.record.value()),
                                       message.committableOffset)
              }
              .toMat(Producer.committableSink(producerDefaults, committerDefaults))(Keep.right)
              .run()
        }
        .to(Sink.seq)
        .run()

      // read copied messages
      val (_, targetConsumer) = createProbe(consumerSettings, targetTopic)
      val copied = targetConsumer
        .request(Messages.size.toLong)
        .expectNextN(Messages.size.toLong)
      copied should contain theSameElementsAs Messages
      targetConsumer.cancel()

      copying.shutdown().futureValue shouldBe Done

      // other consumer in the same group should not receive any
      val (_, sourceConsumer) = createProbe(consumerSettings, topic1)
      sourceConsumer.request(10).expectNoMessage(5.seconds)
      sourceConsumer.cancel()
    }
  }

  def produceStringRoundRobin(topic: String, range: immutable.Seq[String]): Future[Done] =
    Source(range)
    // NOTE: If no partition is specified but a key is present a partition will be chosen
    // using a hash of the key. If neither key nor partition is present a partition
    // will be assigned in a round-robin fashion.
      .map(n => new ProducerRecord[String, String](topic, n))
      .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))

}
