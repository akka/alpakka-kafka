/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.atomic.AtomicLong

import akka.{Done, NotUsed}
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.ProducerMessage.Results
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.pattern.ask
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import akka.util.Timeout
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import org.scalatest._

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Success

class IntegrationSpec extends SpecBase with TestcontainersKafkaLike with Inside {

  implicit val patience = PatienceConfig(30.seconds, 500.millis)

  "Kafka connector" must {
    "produce to plainSink and consume from plainSource" in {
      assertAllStagesStopped {
        val topic1 = createTopic(1)
        val group1 = createGroupId(1)

        Await.result(produce(topic1, 1 to 100), remainingOrDefault)

        val (control, probe) = createProbe(consumerDefaults.withGroupId(group1), topic1)

        probe
          .request(100)
          .expectNextN((1 to 100).map(_.toString))

        probe.cancel()
      }
    }

    "signal rebalance events to actor" in assertAllStagesStopped {
      val partitions = 4
      val totalMessages = 200L

      val topic = createTopic(1, partitions)
      val allTps = (0 until partitions).map(p => new TopicPartition(topic, p))
      val group = createGroupId(1)
      val sourceSettings = consumerDefaults
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        .withGroupId(group)

      val rebalanceActor1 = TestProbe()
      val rebalanceActor2 = TestProbe()

      val receivedCounter = new AtomicLong(0L)

      val topicSubscription = Subscriptions.topics(topic)
      val subscription1 = topicSubscription.withRebalanceListener(rebalanceActor1.ref)
      val subscription2 = topicSubscription.withRebalanceListener(rebalanceActor2.ref)

      def createAndRunConsumer(subscription: Subscription) =
        Consumer
          .plainSource(sourceSettings, subscription)
          .map { el =>
            receivedCounter.incrementAndGet()
            Done
          }
          .scan(0L)((c, _) => c + 1)
          .toMat(Sink.last)(Keep.both)
          .mapMaterializedValue(DrainingControl.apply)
          .run()

      def createAndRunProducer(elements: immutable.Iterable[Long]) =
        Source(elements)
          .map(n => new ProducerRecord(topic, (n % partitions).toInt, DefaultKey, n.toString))
          .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))

      val control = createAndRunConsumer(subscription1)

      // waits until all partitions are assigned to the single consumer
      waitUntilConsumerSummary(group) {
        case singleConsumer :: Nil => singleConsumer.assignment.topicPartitions.size == partitions
      }

      rebalanceActor1.expectMsg(TopicPartitionsAssigned(subscription1, Set(allTps: _*)))

      createAndRunProducer(0L until totalMessages / 2).futureValue

      // create another consumer with the same groupId to trigger re-balancing
      val control2 = createAndRunConsumer(subscription2)

      // waits until partitions are assigned across both consumers
      waitUntilConsumerSummary(group) {
        case consumer1 :: consumer2 :: Nil =>
          val half = partitions / 2
          consumer1.assignment.topicPartitions.size == half && consumer2.assignment.topicPartitions.size == half
      }

      rebalanceActor1.expectMsg(TopicPartitionsRevoked(subscription1, Set(allTps: _*)))

      // The assignment may swap which consumer gets which partitions
      val assigned1 = rebalanceActor1.expectMsgClass(classOf[TopicPartitionsAssigned])
      val assigned2 = rebalanceActor2.expectMsgClass(classOf[TopicPartitionsAssigned])

      val Partitions1 = Set(allTps(0), allTps(1))
      val Partitions2 = Set(allTps(2), allTps(3))
      (assigned1.topicPartitions, assigned2.topicPartitions) match {
        case (Partitions1, Partitions2) =>
        case (Partitions2, Partitions1) =>
        case (receivePartitions1, receivedPartitions2) =>
          fail(
            s"The `TopicPartitionsAssigned` contained different topic partitions than expected:\nrebalanceActor1: $receivePartitions1\nrebalanceActor2: $receivedPartitions2"
          )
      }

      sleep(4.seconds,
            "to get the second consumer started, otherwise it might miss the first messages because of `latest` offset")
      createAndRunProducer(totalMessages / 2 until totalMessages).futureValue

      if (receivedCounter.get() != totalMessages)
        log.warn("All consumers together did receive {}, not the total of {} messages",
                 receivedCounter.get(),
                 totalMessages)

      val stream1messages = control.drainAndShutdown().futureValue
      val stream2messages = control2.drainAndShutdown().futureValue
      if (stream1messages + stream2messages != totalMessages)
        log.warn(
          "The consumers counted {} + {} = {} messages, not the total of {} messages",
          // boxing for Scala 2.11
          Long.box(stream1messages),
          Long.box(stream2messages),
          Long.box(stream1messages + stream2messages),
          Long.box(totalMessages)
        )

      // since Kafka 2.4.0 issued by `consumer.close`
      val revoked1 = rebalanceActor1.expectMsgClass(classOf[TopicPartitionsRevoked])
      val revoked2 = rebalanceActor2.expectMsgClass(classOf[TopicPartitionsRevoked])
      revoked1.topicPartitions shouldBe Partitions1
      revoked2.topicPartitions shouldBe Partitions2
    }

    "connect consumer to producer and commit in batches" in {
      assertAllStagesStopped {
        val topic1 = createTopic(1)
        val topic2 = createTopic(2)
        val group1 = createGroupId(1)

        awaitProduce(produce(topic1, 1 to 10))

        val source = Consumer
          .committableSource(consumerDefaults.withGroupId(group1), Subscriptions.topics(topic1))
          .map(msg => {
            ProducerMessage.single(
              // Produce to topic2
              new ProducerRecord[String, String](topic2, msg.record.value),
              msg.committableOffset
            )
          })
          .via(Producer.flexiFlow(producerDefaults))
          .map(_.passThrough)
          .batch(max = 10, CommittableOffsetBatch.apply)(_.updated(_))
          .mapAsync(producerDefaults.parallelism)(_.commitInternal())

        val probe = source.runWith(TestSink.probe)

        probe.request(1).expectNext()

        probe.cancel()
      }
    }

    class FailingStringSerializer extends StringSerializer {
      override def serialize(topic: String, data: String): Array[Byte] =
        throw new SerializationException()
    }

    "not produce any records after send-failure if stage is stopped" in {
      assertAllStagesStopped {
        val topic1 = createTopic(1)
        val group1 = createGroupId(1)
        // we use a 'max.block.ms' setting that will cause the metadata-retrieval to fail
        // effectively failing the production of the first messages
        val failFirstMessagesProducerSettings =
          ProducerSettings(system, new FailingStringSerializer, new FailingStringSerializer)
            .withBootstrapServers(bootstrapServers)

//        val producer: Future[Option[Results[String, String, NotUsed]]] = Source(1 to 100)
//          .map(n => ProducerMessage.single(new ProducerRecord(topic1, partition0, DefaultKey, n.toString)))
//          .via(Producer.flexiFlow(failFirstMessagesProducerSettings))
//          .runWith(Sink.headOption)
        val producer = produce(topic1, 1 to 100, failFirstMessagesProducerSettings)
        // assure the producer fails as expected
        producer.failed.futureValue shouldBe a[SerializationException]
        //producer.futureValue shouldBe None

        val (control, probe) = createProbe(consumerDefaults.withGroupId(group1), topic1)

        probe
          .request(100)
          .expectNoMessage(1.second)

        probe.cancel()
      }
    }

    "stop and shut down KafkaConsumerActor for committableSource used with take" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)

      Await.ready(produce(topic1, 1 to 10), remainingOrDefault)

      val (control, res) =
        Consumer
          .committableSource(consumerDefaults.withGroupId(group1), Subscriptions.topics(topic1))
          .mapAsync(1)(msg => msg.committableOffset.commitInternal().map(_ => msg.record.value))
          .take(5)
          .toMat(Sink.seq)(Keep.both)
          .run()

      Await.result(control.isShutdown, remainingOrDefault) should be(Done)
      res.futureValue should be((1 to 5).map(_.toString))
    }

    "expose missing groupId as error" in assertAllStagesStopped {
      val topic1 = createTopic(1)

      val control =
        Consumer
          .committableSource(consumerDefaults, Subscriptions.topics(topic1))
          .toMat(Sink.seq)(Keep.both)
          .mapMaterializedValue(DrainingControl.apply)
          .run()

      control.isShutdown.failed.futureValue shouldBe a[org.apache.kafka.common.errors.InvalidGroupIdException]
      control.drainAndShutdown().failed.futureValue shouldBe a[org.apache.kafka.common.errors.InvalidGroupIdException]
    }

    "stop and shut down KafkaConsumerActor for atMostOnceSource used with take" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)

      Await.ready(produce(topic1, 1 to 10), remainingOrDefault)

      val (control, res) =
        Consumer
          .atMostOnceSource(consumerDefaults.withGroupId(group1), Subscriptions.topics(topic1))
          .map(_.value)
          .take(5)
          .toMat(Sink.seq)(Keep.both)
          .run()

      Await.result(control.isShutdown, remainingOrDefault) should be(Done)
      res.futureValue should be((1 to 5).map(_.toString))
    }

    "support metadata fetching on ConsumerActor" in {
      assertAllStagesStopped {
        val topic = createTopic(1)
        val group = createGroupId(1)

        Await.result(produce(topic, 1 to 100), remainingOrDefault)

        // Create ConsumerActor manually
        // https://doc.akka.io/docs/akka-stream-kafka/0.20/consumer.html#sharing-kafkaconsumer
        val consumer = system.actorOf(KafkaConsumerActor.props(consumerDefaults.withGroupId(group)))

        // Timeout for metadata fetching requests
        implicit val timeout: Timeout = Timeout(5.seconds)

        import Metadata._

        // ListTopics
        inside(Await.result(consumer ? ListTopics, remainingOrDefault)) {
          case Topics(Success(topics)) =>
            val pi = topics(topic).head
            assert(pi.topic == topic, "Topic name in retrieved PartitionInfo does not match?!")
        }

        // GetPartitionsFor
        inside(Await.result(consumer ? GetPartitionsFor(topic), remainingOrDefault)) {
          case PartitionsFor(Success(partitions)) =>
            assert(partitions.size == 1, "Topic must have one partition GetPartitionsFor")
            val pi = partitions.head
            assert(pi.topic == topic, "Topic name mismatch in GetPartitionsFor")
            assert(pi.partition == 0, "Partition number mismatch in GetPartitionsFor")
        }

        val partition0 = new TopicPartition(topic, 0)

        // GetBeginningOffsets
        inside(Await.result(consumer ? GetBeginningOffsets(Set(partition0)), remainingOrDefault)) {
          case BeginningOffsets(Success(offsets)) =>
            assert(offsets == Map(partition0 -> 0), "Wrong BeginningOffsets for topic")
        }

        // GetEndOffsets
        inside(Await.result(consumer ? GetEndOffsets(Set(partition0)), remainingOrDefault)) {
          case EndOffsets(Success(offsets)) =>
            assert(offsets == Map(partition0 -> 100), "Wrong EndOffsets for topic")
        }

        val tsYesterday = System.currentTimeMillis() - 86400000

        // GetOffsetsForTimes - beginning
        inside(Await.result(consumer ? GetOffsetsForTimes(Map(partition0 -> tsYesterday)), remainingOrDefault)) {
          case OffsetsForTimes(Success(offsets)) =>
            val offsetAndTs = offsets(partition0)
            assert(offsetAndTs.offset() == 0, "Wrong offset in OffsetsForTimes (beginning)")
        }

        // GetCommittedOffsets
        inside(Await.result(consumer ? GetCommittedOffsets(Set(partition0)), 10.seconds)) {
          case CommittedOffsets(Success(offsetMeta)) =>
            assert(offsetMeta.isEmpty, "Wrong offsets in GetCommittedOffsets")
        }

        // verify that consumption still works
        val probe = Consumer
          .plainExternalSource[Array[Byte], String](consumer, Subscriptions.assignment(partition0))
          .map(_.value())
          .runWith(TestSink.probe)

        probe
          .request(100)
          .expectNextN((1 to 100).map(_.toString))

        probe.cancel()
      }
    }
  }

  "Consumer control" must {

    "complete source when stopped" in
    assertAllStagesStopped {
      val topic = createTopic(1)
      val group = createGroupId(1)

      Await.result(produce(topic, 1 to 100), remainingOrDefault)

      val (control, probe) = Consumer
        .plainSource(consumerDefaults.withGroupId(group), Subscriptions.topics(topic))
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
      val group = createGroupId(1)

      awaitProduce(produce(topic, 1 to 100))

      val (control, probe) = Consumer
        .plainPartitionedSource(consumerDefaults.withGroupId(group).withStopTimeout(10.millis),
                                Subscriptions.topics(topic))
        .flatMapMerge(1, _._2)
        .map(_.value())
        .toMat(TestSink.probe)(Keep.both)
        .run()

      probe
        .request(100)
        .expectNextN(100)

      val stopped = control.stop()
      probe.expectComplete()

      stopped.futureValue should be(Done)

      control.shutdown()
      probe.cancel()
    }

    "access metrics" in assertAllStagesStopped {
      val topic = createTopic(suffix = 1, partitions = 1, replication = 1)
      val group = createGroupId(1)

      val control = Consumer
        .plainSource(consumerDefaults.withGroupId(group), Subscriptions.topics(topic))
        .map(_.value())
        .to(TestSink.probe)
        .run()

      // Wait a tiny bit to avoid a race on "not yet initialized: only setHandler is allowed in GraphStageLogic constructor"
      sleep(1.milli)
      val metrics: Future[Map[MetricName, Metric]] = control.metrics
      metrics.futureValue should not be Symbol("empty")

      Await.result(control.shutdown(), remainingOrDefault)
    }

  }
}
