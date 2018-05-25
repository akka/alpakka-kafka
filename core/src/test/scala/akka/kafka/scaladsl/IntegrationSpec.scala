/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.ConcurrentLinkedQueue

import akka.Done
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka._
import akka.kafka.test.Utils._
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.scalatest._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class IntegrationSpec extends SpecBase(kafkaPort = KafkaPorts.IntegrationSpec) {

  def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort,
      zooKeeperPort,
      Map(
        "offsets.topic.replication.factor" -> "1"
      ))

  "Kafka connector" must {
    "produce to plainSink and consume from plainSource" in {
      assertAllStagesStopped {
        val topic1 = createTopic(1)
        val group1 = createGroup(1)

        givenInitializedTopic(topic1)

        Await.result(produce(topic1, 1 to 100), remainingOrDefault)

        val (control, probe) = createProbe(consumerDefaults.withGroupId(group1), topic1)

        probe
          .request(100)
          .expectNextN((1 to 100).map(_.toString))

        probe.cancel()
      }
    }

    "resume consumer from committed offset" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroup(1)
      val group2 = createGroup(2)

      givenInitializedTopic(topic1)

      // NOTE: If no partition is specified but a key is present a partition will be chosen
      // using a hash of the key. If neither key nor partition is present a partition
      // will be assigned in a round-robin fashion.

      Source(1 to 100)
        .map(n => new ProducerRecord(topic1, partition0, DefaultKey, n.toString))
        .runWith(Producer.plainSink(producerDefaults))

      val committedElements = new ConcurrentLinkedQueue[Int]()

      val consumerSettings = consumerDefaults.withGroupId(group1)

      val (control, probe1) = Consumer.committableSource(consumerSettings, Subscriptions.topics(topic1))
        .filterNot(_.record.value == InitialMsg)
        .mapAsync(10) { elem =>
          elem.committableOffset.commitScaladsl().map { _ =>
            committedElements.add(elem.record.value.toInt)
            Done
          }
        }
        .toMat(TestSink.probe)(Keep.both)
        .run()

      probe1
        .request(25)
        .expectNextN(25).toSet should be(Set(Done))

      probe1.cancel()
      Await.result(control.isShutdown, remainingOrDefault)

      val probe2 = Consumer.committableSource(consumerSettings, Subscriptions.topics(topic1))
        .map(_.record.value)
        .runWith(TestSink.probe)

      // Note that due to buffers and mapAsync(10) the committed offset is more
      // than 26, and that is not wrong

      // some concurrent publish
      Source(101 to 200)
        .map(n => new ProducerRecord(topic1, partition0, DefaultKey, n.toString))
        .runWith(Producer.plainSink(producerDefaults))

      probe2
        .request(100)
        .expectNextN(((committedElements.asScala.max + 1) to 100).map(_.toString))

      probe2.cancel()

      // another consumer should see all
      val probe3 = Consumer.committableSource(consumerSettings.withGroupId(group2), Subscriptions.topics(topic1))
        .filterNot(_.record.value == InitialMsg)
        .map(_.record.value)
        .runWith(TestSink.probe)

      probe3
        .request(100)
        .expectNextN((1 to 100).map(_.toString))

      probe3.cancel()
    }

    "be able to set rebalance listener" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroup(1)

      val consumerSettings = consumerDefaults.withGroupId(group1)

      val listener = TestProbe()

      val sub = Subscriptions.topics(topic1).withRebalanceListener(listener.ref)
      val (control, probe1) = Consumer.committableSource(consumerSettings, sub)
        .filterNot(_.record.value == InitialMsg)
        .mapAsync(10) { elem =>
          elem.committableOffset.commitScaladsl()
        }
        .toMat(TestSink.probe)(Keep.both)
        .run()

      probe1.request(25)

      val revoked = listener.expectMsgType[TopicPartitionsRevoked]
      info("revoked: " + revoked)
      revoked.sub shouldEqual sub
      revoked.topicPartitions.size shouldEqual 0

      val assigned = listener.expectMsgType[TopicPartitionsAssigned]
      info("assigned: " + assigned)
      assigned.sub shouldEqual sub
      assigned.topicPartitions.size shouldEqual 1

      probe1.cancel()
      Await.result(control.isShutdown, remainingOrDefault)
    }

    "handle commit without demand" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroup(1)

      givenInitializedTopic(topic1)

      // important to use more messages than the internal buffer sizes
      // to trigger the intended scenario
      Await.result(produce(topic1, 1 to 100), remainingOrDefault)

      val (control, probe1) = Consumer.committableSource(consumerDefaults.withGroupId(group1), Subscriptions.topics(topic1))
        .filterNot(_.record.value == InitialMsg)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // request one, only
      probe1.request(1)

      val committableOffset = probe1.expectNext().committableOffset

      // enqueue some more
      Await.result(produce(topic1, 101 to 110), remainingOrDefault)

      probe1.expectNoMessage(200.millis)

      // then commit, which triggers a new poll while we haven't drained
      // previous buffer
      val done1 = committableOffset.commitScaladsl()

      Await.result(done1, remainingOrDefault)

      probe1.request(1)
      val done2 = probe1.expectNext().committableOffset.commitScaladsl()
      Await.result(done2, remainingOrDefault)

      probe1.cancel()
      Await.result(control.isShutdown, remainingOrDefault)
    }

    "consume and commit in batches" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroup(1)

      givenInitializedTopic(topic1)

      Await.result(produce(topic1, 1 to 100), remainingOrDefault)
      val consumerSettings = consumerDefaults.withGroupId(group1)

      def consumeAndBatchCommit(topic: String) = {
        Consumer.committableSource(
          consumerSettings,
          Subscriptions.topics(topic)
        )
          .map { msg => msg.committableOffset }
          .batch(max = 10, first => CommittableOffsetBatch(first)) {
            (batch, elem) => batch.updated(elem)
          }
          .mapAsync(1)(_.commitScaladsl())
          .toMat(TestSink.probe)(Keep.both).run()
      }

      val (control, probe) = consumeAndBatchCommit(topic1)

      // Request one batch
      probe.request(1).expectNextN(1)

      probe.cancel()
      Await.result(control.isShutdown, remainingOrDefault)

      // Resume consumption
      val (control2, probe2) = createProbe(consumerSettings, topic1)

      val element = probe2.request(1).expectNext(60.seconds)

      Assertions.assert(element.toInt > 1, "Should start after first element")
      probe2.cancel()
    }

    "connect consumer to producer and commit in batches" in {
      assertAllStagesStopped {
        val topic1 = createTopic(1)
        val topic2 = createTopic(2)
        val group1 = createGroup(1)

        givenInitializedTopic(topic1)

        Await.result(produce(topic1, 1 to 100), remainingOrDefault)

        val source = Consumer.committableSource(consumerDefaults.withGroupId(group1), Subscriptions.topics(topic1))
          .map(msg => {
            ProducerMessage.Message(
              // Produce to topic2
              new ProducerRecord[String, String](topic2, msg.record.value),
              msg.committableOffset
            )
          })
          .via(Producer.flow(producerDefaults))
          .map(_.message.passThrough)
          .batch(max = 10, first => CommittableOffsetBatch(first)) { (batch, elem) =>
            batch.updated(elem)
          }
          .mapAsync(producerDefaults.parallelism)(_.commitScaladsl())

        val probe = source.runWith(TestSink.probe)

        probe.request(1).expectNext()

        probe.cancel()
      }
    }

    "not produce any records after send-failure if stage is stopped" in {
      assertAllStagesStopped {
        val topic1 = createTopic(1)
        val group1 = createGroup(1)
        // we use a 'max.block.ms' setting that will cause the metadata-retrieval to fail
        // effectively failing the production of the first messages
        val failFirstMessagesProducerSettings = producerDefaults.withProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1")

        givenInitializedTopic(topic1)

        Await.ready(produce(topic1, 1 to 100, failFirstMessagesProducerSettings), remainingOrDefault)

        val (control, probe) = createProbe(consumerDefaults.withGroupId(group1), topic1)

        probe
          .request(100)
          .expectNoMessage(1.second)

        probe.cancel()
      }
    }

    "begin consuming from the beginning of the topic" in {
      assertAllStagesStopped {
        val topic = createTopic(1)
        val group = createGroup(1)

        givenInitializedTopic(topic)

        Await.result(produce(topic, 1 to 100), remainingOrDefault)

        val probe = Consumer.plainPartitionedManualOffsetSource(consumerDefaults.withGroupId(group), Subscriptions.topics(topic), _ => Future.successful(Map.empty))
          .flatMapMerge(1, _._2)
          .filterNot(_.value == InitialMsg)
          .map(_.value())
          .runWith(TestSink.probe)

        probe
          .request(100)
          .expectNextN((1 to 100).map(_.toString))

        probe.cancel()
      }
    }

    "begin consuming from the middle of the topic" in {
      assertAllStagesStopped {
        val topic = createTopic(1)
        val group = createGroup(1)

        givenInitializedTopic(topic)

        Await.result(produce(topic, 1 to 100), remainingOrDefault)

        val probe = Consumer.plainPartitionedManualOffsetSource(consumerDefaults.withGroupId(group), Subscriptions.topics(topic), tp => Future.successful(tp.map(_ -> 51L).toMap))
          .flatMapMerge(1, _._2)
          .filterNot(_.value == InitialMsg)
          .map(_.value())
          .runWith(TestSink.probe)

        probe
          .request(50)
          .expectNextN((51 to 100).map(_.toString))

        probe.cancel()
      }
    }

    "call the onRevoked hook" in {
      assertAllStagesStopped {
        val topic = createTopic(1)
        val group = createGroup(1)

        givenInitializedTopic(topic)

        Await.result(produce(topic, 1 to 100), remainingOrDefault)

        var revoked = false

        val source = Consumer.plainPartitionedManualOffsetSource(consumerDefaults.withGroupId(group), Subscriptions.topics(topic), _ => Future.successful(Map.empty), _ => revoked = true)
          .flatMapMerge(1, _._2)
          .filterNot(_.value == InitialMsg)
          .map(_.value())

        val probe1 = source.runWith(TestSink.probe)

        probe1
          .request(50)

        Thread.sleep(consumerDefaults.waitClosePartition.toMillis)

        val probe2 = source.runWith(TestSink.probe)

        eventually(assert(revoked))

        probe1.cancel()
        probe2.cancel()

      }
    }
  }

  "Consumer control" must {

    "complete source when stopped" in
      assertAllStagesStopped {
        val topic = createTopic(1)
        val group = createGroup(1)

        givenInitializedTopic(topic)

        Await.result(produce(topic, 1 to 100), remainingOrDefault)

        val (control, probe) = Consumer.plainSource(consumerDefaults.withGroupId(group), Subscriptions.topics(topic))
          .filterNot(_.value == InitialMsg)
          .map(_.value())
          .toMat(TestSink.probe)(Keep.both)
          .run()

        probe
          .request(100)
          .expectNextN(100)

        val stopped = control.stop()
        probe.expectComplete()

        Await.result(stopped, remainingOrDefault)

        control.shutdown()
        probe.cancel()
      }

    "complete partition sources when stopped" in
      assertAllStagesStopped {
        val topic = createTopic(1)
        val group = createGroup(1)

        givenInitializedTopic(topic)

        Await.result(produce(topic, 1 to 100), remainingOrDefault)

        val (control, probe) = Consumer.plainPartitionedSource(consumerDefaults.withGroupId(group), Subscriptions.topics(topic))
          .flatMapMerge(1, _._2)
          .filterNot(_.value == InitialMsg)
          .map(_.value())
          .toMat(TestSink.probe)(Keep.both)
          .run()

        probe
          .request(100)
          .expectNextN(100)

        val stopped = control.stop()
        probe.expectComplete()

        Await.result(stopped, remainingOrDefault)

        control.shutdown()
        probe.cancel()
      }

  }
}
