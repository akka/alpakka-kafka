/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.ProducerMessage.Message
import akka.kafka.Subscriptions.TopicSubscription
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.test.Utils._
import akka.kafka._
import akka.kafka.test.Utils._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{TestKit, TestProbe}
import akka.{Done, NotUsed}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest._
import org.scalatest.concurrent.Eventually

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class IntegrationSpec extends TestKit(ActorSystem("IntegrationSpec"))
  with WordSpecLike with Matchers with BeforeAndAfterAll
  with BeforeAndAfterEach with TypeCheckedTripleEquals with Eventually {

  implicit val stageStoppingTimeout = StageStoppingTimeout(15.seconds)
  implicit val mat = ActorMaterializer()(system)
  implicit val ec = system.dispatcher
  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2181, Map(
    "offsets.topic.replication.factor" -> "1",
    "offsets.retention.minutes" -> "1",
    "offsets.retention.check.interval.ms" -> "100"
  ))
  val bootstrapServers = s"localhost:${embeddedKafkaConfig.kafkaPort}"

  val DefaultKey = "key"
  val InitialMsg = "initial msg in topic, required to create the topic before any consumer subscribes to it"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    shutdown(system, 30.seconds)
    EmbeddedKafka.stop()
    super.afterAll()
  }

  def uuid = UUID.randomUUID().toString

  def createTopic(number: Int) = s"topic$number-" + uuid

  def createGroup(number: Int) = s"group$number-" + uuid

  val partition0 = 0

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  def givenInitializedTopic(topic: String): Unit = {
    val producer = producerSettings.createKafkaProducer()
    producer.send(new ProducerRecord(topic, partition0, DefaultKey, InitialMsg))
    producer.close(60, TimeUnit.SECONDS)
  }

  /**
   * Produce messages to topic using specified range and return
   * a Future so the caller can synchronize consumption.
   */
  def produce(topic: String, range: Range, settings: ProducerSettings[String, String] = producerSettings): Future[Done] = {
    val source = Source(range)
      .map(n => {
        val record = new ProducerRecord(topic, partition0, DefaultKey, n.toString)

        Message(record, NotUsed)
      })
      .viaMat(Producer.flow(settings))(Keep.right)

    source.runWith(Sink.ignore)
  }

  def createConsumerSettings(group: String): ConsumerSettings[String, String] = {
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId(group)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withWakeupTimeout(10 seconds)
      .withMaxWakeups(10)
  }

  def createProbe(
    consumerSettings: ConsumerSettings[String, String],
    topic: String
  ): TestSubscriber.Probe[String] = {
    Consumer.plainSource(consumerSettings, Subscriptions.topics(Set(topic)))
      .filterNot(_.value == InitialMsg)
      .map(_.value)
      .runWith(TestSink.probe)
  }

  "Reactive kafka streams" must {
    "produce to plainSink and consume from plainSource" in {
      assertAllStagesStopped {
        val topic1 = createTopic(1)
        val group1 = createGroup(1)

        givenInitializedTopic(topic1)

        Await.result(produce(topic1, 1 to 100), remainingOrDefault)

        val consumerSettings = createConsumerSettings(group1)
        val probe = createProbe(consumerSettings, topic1)

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
        .runWith(Producer.plainSink(producerSettings))

      val committedElements = new ConcurrentLinkedQueue[Int]()

      val consumerSettings = createConsumerSettings(group1)

      val (control, probe1) = Consumer.committableSource(consumerSettings, Subscriptions.topics(Set(topic1)))
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

      val probe2 = Consumer.committableSource(consumerSettings, Subscriptions.topics(Set(topic1)))
        .map(_.record.value)
        .runWith(TestSink.probe)

      // Note that due to buffers and mapAsync(10) the committed offset is more
      // than 26, and that is not wrong

      // some concurrent publish
      Source(101 to 200)
        .map(n => new ProducerRecord(topic1, partition0, DefaultKey, n.toString))
        .runWith(Producer.plainSink(producerSettings))

      probe2
        .request(100)
        .expectNextN(((committedElements.asScala.max + 1) to 100).map(_.toString))

      probe2.cancel()

      // another consumer should see all
      val probe3 = Consumer.committableSource(consumerSettings.withGroupId(group2), Subscriptions.topics(Set(topic1)))
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

      val consumerSettings = createConsumerSettings(group1)

      val listener = TestProbe()

      val sub = Subscriptions.topics(Set(topic1)).withRebalanceListener(listener.ref)
      val (control, probe1) = Consumer.committableSource(consumerSettings, sub)
        .filterNot(_.record.value == InitialMsg)
        .mapAsync(10) { elem =>
          elem.committableOffset.commitScaladsl().map { _ ⇒ Done }
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

    "resume consumer from committed offset after retention period" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroup(1)
      val group2 = createGroup(2)

      givenInitializedTopic(topic1)

      // NOTE: If no partition is specified but a key is present a partition will be chosen
      // using a hash of the key. If neither key nor partition is present a partition
      // will be assigned in a round-robin fashion.

      Source(1 to 100)
        .map(n => new ProducerRecord(topic1, partition0, DefaultKey, n.toString))
        .runWith(Producer.plainSink(producerSettings))

      val committedElements = new ConcurrentLinkedQueue[Int]()

      val consumerSettings = createConsumerSettings(group1).withCommitRefreshInterval(5.seconds)

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

      Thread.sleep(70000)

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
        .runWith(Producer.plainSink(producerSettings))

      probe2
        .request(100)
        .expectNextN(((committedElements.asScala.max + 1) to 100).map(_.toString))

      Thread.sleep(70000)

      probe2.cancel()

      val probe3 = Consumer.committableSource(consumerSettings, Subscriptions.topics(topic1))
        .map(_.record.value)
        .runWith(TestSink.probe)

      probe3
        .request(100)
        .expectNextN(((committedElements.asScala.max + 1) to 100).map(_.toString))

      probe3.cancel()
    }

    "handle commit without demand" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroup(1)

      givenInitializedTopic(topic1)

      // important to use more messages than the internal buffer sizes
      // to trigger the intended scenario
      Await.result(produce(topic1, 1 to 100), remainingOrDefault)

      val consumerSettings = createConsumerSettings(group1)

      val (control, probe1) = Consumer.committableSource(consumerSettings, Subscriptions.topics(Set(topic1)))
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
      val consumerSettings = createConsumerSettings(group1)

      def consumeAndBatchCommit(topic: String) = {
        Consumer.committableSource(
          consumerSettings,
          Subscriptions.topics(Set(topic))
        )
          .map { msg => msg.committableOffset }
          .batch(max = 10, first => CommittableOffsetBatch.empty.updated(first)) {
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
      val consumerSettings2 = createConsumerSettings(group1)
      val probe2 = createProbe(consumerSettings2, topic1)

      val element = probe2.request(1).expectNext(60 seconds)

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

        val consumerSettings1 = createConsumerSettings(group1)

        val source = Consumer.committableSource(consumerSettings1, Subscriptions.topics(Set(topic1)))
          .map(msg => {
            ProducerMessage.Message(
              // Produce to topic2
              new ProducerRecord[String, String](topic2, msg.record.value),
              msg.committableOffset
            )
          })
          .via(Producer.flow(producerSettings))
          .map(_.message.passThrough)
          .batch(max = 10, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
            batch.updated(elem)
          }
          .mapAsync(producerSettings.parallelism)(_.commitScaladsl())

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
        val failFirstMessagesProducerSettings = producerSettings.withProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1")

        givenInitializedTopic(topic1)

        Await.ready(produce(topic1, 1 to 100, failFirstMessagesProducerSettings), remainingOrDefault)

        val consumerSettings = createConsumerSettings(group1)

        val probe = createProbe(consumerSettings, topic1)

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

        val consumerSettings = createConsumerSettings(group)

        val probe = Consumer.plainPartitionedManualOffsetSource(consumerSettings, Subscriptions.topics(Set(topic)), _ => Future.successful(Map.empty))
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

        val consumerSettings = createConsumerSettings(group)

        val probe = Consumer.plainPartitionedManualOffsetSource(consumerSettings, Subscriptions.topics(Set(topic)), tp => Future.successful(tp.map(_ -> 51L).toMap))
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

        val consumerSettings = createConsumerSettings(group)

        var revoked = false

        val source = Consumer.plainPartitionedManualOffsetSource(consumerSettings, Subscriptions.topics(Set(topic)), _ => Future.successful(Map.empty), _ => revoked = true)
          .flatMapMerge(1, _._2)
          .filterNot(_.value == InitialMsg)
          .map(_.value())

        val probe1 = source.runWith(TestSink.probe)

        probe1
          .request(50)

        val probe2 = source.runWith(TestSink.probe)

        eventually(assert(revoked))

        probe1.cancel()
        probe2.cancel()

      }
    }

    "complete source when control stopped" in {
      assertAllStagesStopped {
        val topic = createTopic(1)
        val group = createGroup(1)

        givenInitializedTopic(topic)

        Await.result(produce(topic, 1 to 100), remainingOrDefault)

        val consumerSettings = createConsumerSettings(group)

        val (control, probe) = Consumer.plainSource(consumerSettings, Subscriptions.topics(Set(topic)))
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

    "complete partition sources when the main source control stopped" in {
      assertAllStagesStopped {
        val topic = createTopic(1)
        val group = createGroup(1)

        givenInitializedTopic(topic)

        Await.result(produce(topic, 1 to 100), remainingOrDefault)

        val consumerSettings = createConsumerSettings(group)

        val (control, probe) = Consumer.plainPartitionedSource(consumerSettings, Subscriptions.topics(Set(topic)))
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

    "complete a consume-transform-produce transaction" in {
      assertAllStagesStopped {
        val sourceTopic = createTopic(1)
        val sinkTopic = createTopic(2)
        val group = createGroup(1)

        givenInitializedTopic(sourceTopic)
        givenInitializedTopic(sinkTopic)

        Await.result(produce(sourceTopic, 1 to 100), remainingOrDefault)

        val consumerSettings = createConsumerSettings(group)

        val control = Consumer.transactionalSource(consumerSettings, TopicSubscription(Set(sourceTopic), None))
          .filterNot(_.record.value() == InitialMsg)
          .map { msg =>
            ProducerMessage.Message(
              new ProducerRecord[Array[Byte], String](sinkTopic, msg.record.value), msg.partitionOffset)
          }
          .via(Producer.transactionalFlow(producerSettings, group))
          .toMat(Sink.ignore)(Keep.left)
          .run()

        val probeConsumerGroup = createGroup(2)
        val probeConsumerSettings = createConsumerSettings(probeConsumerGroup)
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

        val consumerSettings = createConsumerSettings(group)

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
                Thread.sleep(producerSettings.eosCommitInterval.toMillis + 10)
              }
              if (msg.record.value().toInt == 501 && restartCount < 2) {
                throw new RuntimeException("Uh oh..")
              }
              else {
                ProducerMessage.Message(
                  new ProducerRecord[Array[Byte], String](sinkTopic, msg.record.value()), msg.partitionOffset)
              }
            }
            // side effect out the `Control` materialized value because it can't be propagated through the `RestartSource`
            .mapMaterializedValue(innerControl = _)
            .via(Producer.transactionalFlow(producerSettings, group))
        }

        restartSource.runWith(Sink.ignore)

        val probeGroup = createGroup(2)
        val probeConsumerSettings = createConsumerSettings(probeGroup)
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
