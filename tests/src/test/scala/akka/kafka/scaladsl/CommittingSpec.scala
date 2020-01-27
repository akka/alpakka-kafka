/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntUnaryOperator

import akka.actor.ActorRef
import akka.kafka.ConsumerMessage.{CommittableOffsetBatch, GroupTopicPartition}
import akka.kafka.ProducerMessage.MultiMessage
import akka.kafka._
import akka.kafka.internal.CommittableOffsetBatchImpl
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest._

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

class CommittingSpec extends SpecBase with TestcontainersKafkaLike with Inside {

  implicit val patience: PatienceConfig = PatienceConfig(30.seconds, 500.millis)

  final val Numbers = (1 to 200).map(_.toString)
  final val partition1 = 1

  "Committing" must {

    "ensure uncommitted messages are redelivered" in assertAllStagesStopped {
      val Messages = Numbers.take(100)
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)
      val group2 = createGroupId(2)

      produceString(topic1, Messages)

      val committedElements = new AtomicInteger()

      val consumerSettings = consumerDefaults.withGroupId(group1)

      val (control, probe1) = Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic1))
        .mapAsync(10) { elem =>
          elem.committableOffset.commitInternal().map { _ =>
            committedElements.updateAndGet(new IntUnaryOperator {
              override def applyAsInt(operand: Int): Int = Math.max(operand, elem.record.value.toInt)
            })
            elem.record.value
          }
        }
        .toMat(TestSink.probe)(Keep.both)
        .run()

      probe1
        .request(25)
        .expectNextN(Messages.take(25))

      probe1.cancel()
      control.isShutdown.futureValue shouldBe Done

      val probe2 = Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic1))
        .map(_.record.value)
        .runWith(TestSink.probe)

      // Note that due to buffers and mapAsync(10) the committed offset is more
      // than 26, and that is not wrong

      // some concurrent publish
      produceString(topic1, Messages.drop(100))

      probe2
        .request(Messages.size.toLong)
        .expectNextN(Messages.drop(committedElements.get()))

      probe2.cancel()

      // another consumer should see all
      val probe3 = Consumer
        .committableSource(consumerSettings.withGroupId(group2), Subscriptions.topics(topic1))
        .map(_.record.value)
        .runWith(TestSink.probe)

      probe3
        .request(100)
        .expectNextN(Messages.take(100))

      probe3.cancel()
    }

    "work even if the partition gets balanced away and is not reassigned yet (#750)" in assertAllStagesStopped {
      val count = 10
      val topic1 = createTopic(1, partitions = 2)
      val group1 = createGroupId(1)
      val consumerSettings = consumerDefaults
        .withGroupId(group1)

      Source(Numbers.take(count))
        .map { n =>
          MultiMessage(List(
                         new ProducerRecord(topic1, partition0, DefaultKey, n + "-p0"),
                         new ProducerRecord(topic1, partition1, DefaultKey, n + "-p1")
                       ),
                       NotUsed)
        }
        .via(Producer.flexiFlow(producerDefaults.withProducer(testProducer)))
        .runWith(Sink.ignore)

      // Subscribe to the topic (without demand)
      val rebalanceActor1 = TestProbe()
      val subscription1 = Subscriptions.topics(topic1).withRebalanceListener(rebalanceActor1.ref)
      val (control1, probe1) = Consumer
        .committableSource(consumerSettings, subscription1)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // Await initial partition assignment
      rebalanceActor1.expectMsg(
        TopicPartitionsAssigned(subscription1,
                                Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )

      // read all messages from both partitions
      val committables1: immutable.Seq[ConsumerMessage.CommittableMessage[String, String]] = probe1
        .request(count * 2L)
        .expectNextN(count * 2L)

      // Subscribe to the topic (without demand)
      val rebalanceActor2 = TestProbe()
      val subscription2 = Subscriptions.topics(topic1).withRebalanceListener(rebalanceActor2.ref)
      val (control2, probe2) = Consumer
        .committableSource(consumerSettings, subscription2)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // Await an assignment to the new rebalance listener
      rebalanceActor2.expectMsg(
        TopicPartitionsAssigned(subscription2, Set(new TopicPartition(topic1, partition1)))
      )
      // Await revoke of all partitions in old rebalance listener
      rebalanceActor1.expectMsg(
        TopicPartitionsRevoked(subscription1,
                               Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )

      // commit BEFORE the reassign finishes with an assignment
      val consumer1Read = Future.sequence(
        committables1
          .map { elem =>
            elem.committableOffset.commitInternal().map { _ =>
              elem.record.value
            }
          }
      )

      // the rebalance finishes
      rebalanceActor1.expectMsg(TopicPartitionsAssigned(subscription1, Set(new TopicPartition(topic1, partition0))))

      // before the fix // ... but committing failed
      // val commitFailed = consumer1Read.failed.futureValue
      // commitFailed shouldBe a[CommitFailedException]
      consumer1Read.futureValue should contain theSameElementsAs {
        Numbers.take(count).map(_ + "-p1") ++
        Numbers.take(count).map(_ + "-p0")
      }

      probe1.cancel()
      probe2.cancel()

      control1.isShutdown.futureValue shouldBe Done
      control2.isShutdown.futureValue shouldBe Done
    }

    // This test shows that Kafka ignores commits that reach the consumer after a partition
    // got revoked from it. Those commits may be enqueued already, but won't have an effect.
    "ignore commits to partitions that got revoked" in assertAllStagesStopped {
      val count = 10
      val topic1 = createTopic(1, partitions = 2)
      val group1 = createGroupId(1)
      val consumerSettings = consumerDefaults
        .withGroupId(group1)

      Source(Numbers.take(count))
        .map { n =>
          MultiMessage(List(
                         new ProducerRecord(topic1, partition0, DefaultKey, n + "-p0"),
                         new ProducerRecord(topic1, partition1, DefaultKey, n + "-p1")
                       ),
                       NotUsed)
        }
        .via(Producer.flexiFlow(producerDefaults.withProducer(testProducer)))
        .runWith(Sink.ignore)

      // Subscribe to the topic (without demand)
      val rebalanceActor1 = TestProbe()
      val subscription1 = Subscriptions.topics(topic1).withRebalanceListener(rebalanceActor1.ref)
      val (control1, probe1) = Consumer
        .committableSource(consumerSettings, subscription1)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // Await initial partition assignment
      rebalanceActor1.expectMsg(
        TopicPartitionsAssigned(subscription1,
                                Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )

      // read all messages from both partitions
      val committables1: immutable.Seq[ConsumerMessage.CommittableMessage[String, String]] = probe1
        .request(count * 2L)
        .expectNextN(count * 2L)

      // Subscribe to the topic (without demand)
      val rebalanceActor2 = TestProbe()
      val subscription2 = Subscriptions.topics(topic1).withRebalanceListener(rebalanceActor2.ref)
      val (control2, probe2) = Consumer
        .committableSource(consumerSettings, subscription2)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // Rebalance happens
      rebalanceActor1.expectMsg(
        TopicPartitionsRevoked(subscription1,
                               Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )
      rebalanceActor1.expectMsg(TopicPartitionsAssigned(subscription1, Set(new TopicPartition(topic1, partition0))))
      rebalanceActor2.expectMsg(TopicPartitionsAssigned(subscription2, Set(new TopicPartition(topic1, partition1))))

      // commit ALL messages via consumer 1
      Future.sequence(
        committables1
          .map { elem =>
            elem.committableOffset.commitInternal().map { _ =>
              elem.record.value
            }
          }
      )

      val committables2: immutable.Seq[ConsumerMessage.CommittableMessage[String, String]] = probe2
        .request(count.toLong)
        .expectNextN(count.toLong)

      // messages that belonged to the revoked partition show up in the new consumer, even though they were
      // committed after the rebalance
      committables2.map(_.record.value()) should contain theSameElementsInOrderAs {
        Numbers.take(count).map(_ + "-p1")
      }

      probe1.cancel()
      probe2.cancel()

      control1.isShutdown.futureValue shouldBe Done
      control2.isShutdown.futureValue shouldBe Done
    }

    "work without demand" in assertAllStagesStopped {
      val topic = createTopic()
      val group = createGroupId()

      // important to use more messages than the internal buffer sizes
      // to trigger the intended scenario
      produce(topic, 1 to 100).futureValue shouldBe Done

      val (control, probe1) = Consumer
        .committableSource(consumerDefaults.withGroupId(group), Subscriptions.topics(topic))
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // request one, only
      probe1.request(1)

      val committableOffset = probe1.expectNext().committableOffset

      // enqueue some more
      produce(topic, 101 to 110).futureValue shouldBe Done

      probe1.expectNoMessage(200.millis)

      // then commit, which triggers a new poll while we haven't drained
      // previous buffer
      committableOffset.commitInternal().futureValue shouldBe Done

      probe1.request(1)
      probe1.expectNext().committableOffset.commitInternal().futureValue shouldBe Done

      probe1.cancel()
      control.isShutdown.futureValue shouldBe Done
    }

    // See equal test "Backpressure-free committing" must "allow commit batches"
    "work in batches" in assertAllStagesStopped {
      val topic = createTopic()
      val group = createGroupId()

      produce(topic, 1 to 100).futureValue shouldBe Done
      val consumerSettings = consumerDefaults.withGroupId(group)

      def consumeAndBatchCommit(topic: String) =
        Consumer
          .committableSource(
            consumerSettings,
            Subscriptions.topics(topic)
          )
          .map(_.committableOffset)
          .batch(max = 10, CommittableOffsetBatch.apply)(_.updated(_))
          .mapAsync(1)(_.commitInternal())
          .toMat(TestSink.probe)(Keep.both)
          .run()

      val (control, probe) = consumeAndBatchCommit(topic)

      // Request one batch
      probe.request(1).expectNextN(1)

      probe.cancel()
      control.isShutdown.futureValue shouldBe Done

      val element = consumeFirstElement(topic, consumerSettings)
      assert(element.toInt > 1, "Should start after first element")
    }

    "work with a committer sink" in assertAllStagesStopped {
      val topic = createTopic()
      val group = createGroupId()

      awaitProduce(produce(topic, 1 to 100))
      val consumerSettings = consumerDefaults.withGroupId(group)
      val committerSettings = committerDefaults.withMaxBatch(5)

      def consumeAndCommitUntil(topic: String, failAt: String) =
        Consumer
          .committableSource(
            consumerSettings,
            Subscriptions.topics(topic)
          )
          .map {
            case msg if msg.record.value() == failAt => throw new Exception
            case other => other
          }
          .map(_.committableOffset)
          .toMat(Committer.sink(committerSettings))(Keep.right)
          .run()

      // Consume and fail in the middle of the commit batch
      val failAt = 32
      consumeAndCommitUntil(topic, failAt.toString).failed.futureValue shouldBe an[Exception]

      val element1 = consumeFirstElement(topic, consumerSettings)
      assert(element1.toInt >= failAt - committerSettings.maxBatch, "Should re-process at most maxBatch elements")
    }

  }

  "Multiple consumers to one committer" must {
    "merge commits with different group IDs" in assertAllStagesStopped {
      val topic = createTopic()
      val group1 = createGroupId()
      val group2 = createGroupId()

      awaitProduce(produce(topic, 1 to 10))
      val subscription = Subscriptions.topics(topic)
      val result =
        Consumer
          .committableSource(consumerDefaults.withGroupId(group1), subscription)
          .merge(Consumer.committableSource(consumerDefaults.withGroupId(group2), subscription))
          .map(_.committableOffset)
          .groupedWithin(20, 10.seconds)
          .map(CommittableOffsetBatch.apply)
          .via(Committer.batchFlow(committerDefaults))
          .take(1L)
          .runWith(Sink.head)

      val batch = result.mapTo[CommittableOffsetBatchImpl].futureValue
      batch.batchSize shouldBe 20
      batch.offsets should contain allElementsOf Map(
        GroupTopicPartition(group1, topic, 0) -> 9,
        GroupTopicPartition(group2, topic, 0) -> 9
      )

      // make sure committing was done for both group IDs
      val res1 = expectNoElements(group1, topic)
      val res2 = expectNoElements(group2, topic)

      res1.failed.futureValue shouldBe a[java.util.concurrent.TimeoutException]
      res2.failed.futureValue shouldBe a[java.util.concurrent.TimeoutException]
    }

    "allow a shared consumer actor" in assertAllStagesStopped {
      val topic = createTopic(0, partitions = 2)
      val group = createGroupId()

      awaitProduce(produceTwoPartitions(topic))

      val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerDefaults.withGroupId(group)))
      val result =
        Consumer
          .committableExternalSource(consumer,
                                     Subscriptions.assignment(new TopicPartition(topic, partition0)),
                                     group,
                                     consumerDefaults.commitTimeout)
          .merge(
            Consumer
              .committableExternalSource(consumer,
                                         Subscriptions.assignment(new TopicPartition(topic, partition1)),
                                         group,
                                         consumerDefaults.commitTimeout)
          )
          .map(_.committableOffset)
          .groupedWithin(20, 5.seconds)
          .map(CommittableOffsetBatch.apply)
          .via(Committer.batchFlow(committerDefaults))
          .take(1L)
          .runWith(Sink.head)

      val batch = result.mapTo[CommittableOffsetBatchImpl].futureValue
      batch.batchSize shouldBe 20
      batch.offsets should contain allElementsOf Map(
        GroupTopicPartition(group, topic, partition0) -> 9,
        GroupTopicPartition(group, topic, partition1) -> 9
      )

      // make sure committing was done
      val res = expectNoElements(group, topic)
      res.failed.futureValue shouldBe a[java.util.concurrent.TimeoutException]
    }

    // Illustration of https://github.com/akka/alpakka-kafka/issues/942
    "merge multiple consumers' offsets for different partitions (#942)" in assertAllStagesStopped {
      val topic = createTopic(0, partitions = 2)
      val group = createGroupId()

      awaitProduce(produceTwoPartitions(topic))
      val consumerSettings = consumerDefaults.withGroupId(group)
      val result =
        Consumer
          .committableSource(
            consumerSettings
              .withStopTimeout(100.millis), // this consumer actor needs to stay around to receive the commit
            Subscriptions.assignment(new TopicPartition(topic, partition0))
          )
          .take(10)
          // triggers commit timeout as the actor is terminated
          .delay(50.millis)
          .concat(
            Consumer
              .committableSource(consumerSettings, Subscriptions.assignment(new TopicPartition(topic, partition1)))
          )
          .map(_.committableOffset)
          .groupedWithin(20, 10.seconds)
          .map(CommittableOffsetBatch.apply)
          .log("sending offset batch")
          .via(Committer.batchFlow(committerDefaults.withMaxBatch(1L)))
          .log("offset batch done")
          .runWith(Sink.head)

      val batch = result.mapTo[CommittableOffsetBatchImpl].futureValue
      batch.batchSize shouldBe 20
      batch.offsets should contain allElementsOf Map(
        GroupTopicPartition(group, topic, partition0) -> 9,
        GroupTopicPartition(group, topic, partition1) -> 9
      )

      // make sure committing was done
      val res = expectNoElements(group, topic)
      res.failed.futureValue shouldBe a[java.util.concurrent.TimeoutException]
    }

    // Illustration of https://github.com/akka/alpakka-kafka/issues/942
    "merge multiple consumer` with identical group-topic-subscription (#942)" in assertAllStagesStopped {
      val topic = createTopic()
      val group = createGroupId()

      val messages = 20
      val messagesInFirstIncarnation = 11
      awaitProduce(produce(topic, 1 to messages))
      val consumerSettings =
        consumerDefaults.withGroupId(group).withCommitWarning(1.second).withCommitTimeout(5.seconds)
      val subscription = Subscriptions.topics(topic)

      val incarnation = new AtomicInteger()
      val result =
        RestartSource
          .withBackoff(0.millis, 5.millis, 0d) { () =>
            if (incarnation.incrementAndGet() == 1)
              Consumer
                .committableSource(consumerSettings, subscription)
                .log("incarnation 1")
                .take(messagesInFirstIncarnation.toLong)
            else
              Consumer
                .committableSource(consumerSettings, subscription)
                .log("incarnation 2")
          }
          .map(_.committableOffset)
          .groupedWithin(messagesInFirstIncarnation + messages, 10.seconds)
          .map(CommittableOffsetBatch.apply)
          .via(Committer.batchFlow(committerDefaults.withMaxBatch(1L)))
          .runWith(Sink.head)

      val batch = result.mapTo[CommittableOffsetBatchImpl].futureValue
      batch.batchSize shouldBe messagesInFirstIncarnation + messages
      batch.offsets should contain allElementsOf Map(
        GroupTopicPartition(group, topic, partition0) -> 19
      )

      // make sure committing was done
      val res = expectNoElements(group, topic)
      res.failed.futureValue shouldBe a[java.util.concurrent.TimeoutException]
    }
  }

  "Backpressure-free committing" must {
    "allow commit batches" in assertAllStagesStopped {
      val topic = createTopic()
      val group = createGroupId()
      val commitBatchSize = 10L

      produce(topic, 1 to 100).futureValue shouldBe Done
      val consumerSettings = consumerDefaults.withGroupId(group)

      def consumeAndBatchCommit(topic: String) =
        Consumer
          .committableSource(
            consumerSettings,
            Subscriptions.topics(topic)
          )
          .map(_.committableOffset)
          .via(
            Committer
              .batchFlow(committerDefaults.withDelivery(CommitDelivery.SendAndForget).withMaxBatch(commitBatchSize))
          )
          .toMat(TestSink.probe)(Keep.both)
          .run()

      val (control, probe) = consumeAndBatchCommit(topic)

      // Request two commit batches
      val committableOffsetBatches = probe.request(2).expectNextN(2)
      committableOffsetBatches(0).batchSize shouldBe commitBatchSize
      committableOffsetBatches(1).batchSize shouldBe commitBatchSize

      probe.cancel()
      control.isShutdown.futureValue shouldBe Done

      val element = consumeFirstElement(topic, consumerSettings)
      assert(element.toInt > (commitBatchSize * 2) - 1, "Should start after the already committed messages")
    }
  }

  private def consumeFirstElement(topic: String, consumerSettings: ConsumerSettings[String, String]): String = {
    val (_, probe2) = createProbe(consumerSettings, topic)
    val element = probe2.request(1).expectNext(60.seconds)
    probe2.cancel()
    element
  }

  private def expectNoElements(groupId: String, topic: String) =
    Consumer
      .plainSource(consumerDefaults.withGroupId(groupId), Subscriptions.topics(topic))
      .idleTimeout(500.millis)
      .runWith(Sink.head)

  private def produceTwoPartitions(topic: String) =
    Source(1 to 10)
      .map(_.toString)
      .mapConcat(
        n =>
          immutable.Seq(new ProducerRecord(topic, partition0, DefaultKey, n),
                        new ProducerRecord(topic, partition1, DefaultKey, n))
      )
      .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))
}
