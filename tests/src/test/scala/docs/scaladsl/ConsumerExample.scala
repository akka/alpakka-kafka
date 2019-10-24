/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl._
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Flow, Keep, RestartSource, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

// Consume messages and store a representation, including offset, in DB
class ConsumerExample extends DocsSpecBase with TestcontainersKafkaLike {

  override def sleepAfterProduce: FiniteDuration = 4.seconds
  private def waitBeforeValidation(): Unit = sleep(4.seconds)

  "ExternalOffsetStorage" should "work" in assertAllStagesStopped {
    val topic = createTopic()
    val consumerSettings = consumerDefaults.withClientId("externalOffsetStorage")
    // format: off
    // #plainSource
    val db = new OffsetStore
    val control = db.loadOffset().map { fromOffset =>
      Consumer
        .plainSource(
          consumerSettings,
          Subscriptions.assignmentWithOffset(
            new TopicPartition(topic, /* partition = */ 0) -> fromOffset
          )
        )
        .mapAsync(1)(db.businessLogicAndStoreOffset)
        .toMat(Sink.seq)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
    }
    // #plainSource
    awaitProduce(produce(topic, 1 to 10))
    control.flatMap(_.drainAndShutdown()).futureValue should have size (10)
  }

  // #plainSource

  class OffsetStore {
    // #plainSource

    private val offset = new AtomicLong

  // #plainSource
    def businessLogicAndStoreOffset(record: ConsumerRecord[String, String]): Future[Done] = // ...
      // #plainSource
      {
        println(s"DB.save: ${record.value}")
        offset.set(record.offset)
        Future.successful(Done)
      }

  // #plainSource
    def loadOffset(): Future[Long] = // ...
      // #plainSource
      Future.successful(offset.get)

  // #plainSource
  }
  // #plainSource
  // format: on

  def createSettings(): ConsumerSettings[String, Array[Byte]] = {
    // #settings
    val config = system.settings.config.getConfig("akka.kafka.consumer")
    val consumerSettings =
      ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
        .withBootstrapServers(bootstrapServers)
        .withGroupId("group1")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    //#settings
    consumerSettings
  }

  def createAutoCommitSettings(): ConsumerSettings[String, Array[Byte]] = {
    val consumerSettings = createSettings()
    val consumerSettingsWithAutoCommit =
      // #settings-autocommit
      consumerSettings
        .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")
    // #settings-autocommit
    consumerSettingsWithAutoCommit
  }

  "ConsumerSettings" should "read from settings that inherit default" in {
    // #config-inheritance
    val config = system.settings.config.getConfig("our-kafka-consumer")
    val consumerSettings = ConsumerSettings(config, new StringDeserializer, new StringDeserializer)
    // #config-inheritance
    consumerSettings.getProperty("bootstrap.servers") shouldBe "kafka-host:9092"
  }

  "Consume messages at-most-once" should "work" in assertAllStagesStopped {
    val consumerSettings = createSettings().withGroupId(createGroupId())
    val topic = createTopic()
    val totalMessages = 10
    val lastMessage = Promise[Done]

    def business(key: String, value: Array[Byte]): Future[Done] = {
      if (value.toList == totalMessages.toString.getBytes.toList) lastMessage.success(Done)
      Future.successful(Done)
    }

    // #atMostOnce
    val control: DrainingControl[immutable.Seq[Done]] =
      Consumer
        .atMostOnceSource(consumerSettings, Subscriptions.topics(topic))
        .mapAsync(1)(record => business(record.key, record.value()))
        .toMat(Sink.seq)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
    // #atMostOnce

    awaitProduce(produce(topic, 1 to totalMessages))
    lastMessage.future
      .flatMap(_ => control.drainAndShutdown())
      .futureValue should have size (10)
  }
  // #atMostOnce

  def business(key: String, value: String): Future[Done] = // ???
    // #atMostOnce
    Future.successful(Done)

  def business(rec: ConsumerRecord[String, String]): Future[Done] = Future.successful(Done)

  "Consume messages at-least-once" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic()
    // #atLeastOnce
    val control =
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic))
        .mapAsync(10) { msg =>
          business(msg.record.key, msg.record.value).map(_ => msg.committableOffset)
        }
        .via(Committer.flow(committerDefaults.withMaxBatch(1)))
        .toMat(Sink.seq)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
    // #atLeastOnce
    awaitProduce(produce(topic, 1 to 10))
    Await.result(control.drainAndShutdown(), 5.seconds) should have size (10)
  }
  // format: off
  // #atLeastOnce

  def business(key: String, value: Array[Byte]): Future[Done] = // ???

  // #atLeastOnce
    Future.successful(Done)
  // format: on

  it should "support withOffsetContext" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic()
    val control =
      Consumer
        .sourceWithOffsetContext(consumerSettings, Subscriptions.topics(topic))
        .mapAsync(10) { record =>
          business(record.key, record.value)
        }
        .via(Committer.flowWithOffsetContext(committerDefaults))
        .toMat(Sink.seq)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
    awaitProduce(produce(topic, 1 to 10))
    control.drainAndShutdown().futureValue.map { case (_, b) => b.batchSize }.sum shouldBe 10
  }

  "Consume messages at-least-once, and commit in batches" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic()
    // #commitWithMetadata
    def metadataFromRecord(record: ConsumerRecord[String, String]): String =
      record.timestamp().toString

    val control =
      Consumer
        .commitWithMetadataSource(consumerSettings, Subscriptions.topics(topic), metadataFromRecord)
        .mapAsync(1) { msg =>
          business(msg.record.key, msg.record.value)
            .map(_ => msg.committableOffset)
        }
        .toMat(Committer.sink(committerDefaults))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
    // #commitWithMetadata
    awaitProduce(produce(topic, 1 to 10))
    control.drainAndShutdown().futureValue should be(Done)
  }

  "Consume messages at-least-once, and commit with a committer sink" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic()
    // #committerSink
    val committerSettings = CommitterSettings(system)

    val control: DrainingControl[Done] =
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic))
        .mapAsync(1) { msg =>
          business(msg.record.key, msg.record.value)
            .map(_ => msg.committableOffset)
        }
        .toMat(Committer.sink(committerSettings))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
    // #committerSink
    awaitProduce(produce(topic, 1 to 10))
    Await.result(control.drainAndShutdown(), 5.seconds)
  }

  "Connect a Consumer to Producer" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic1 = createTopic(1)
    val topic2 = createTopic(2)
    val targetTopic = createTopic(3)
    val producerSettings = producerDefaults
    val committerSettings = committerDefaults
    //format: off
    // #consumerToProducerSink
    val control =
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic1, topic2))
        .map { msg =>
          ProducerMessage.single(
            new ProducerRecord(targetTopic, msg.record.key, msg.record.value),
            msg.committableOffset
          )
        }
        .toMat(Producer.committableSink(producerSettings, committerSettings))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
    // #consumerToProducerSink
    //format: on
    awaitProduce(produce(topic1, 1 to 10), produce(topic2, 1 to 10))
    Await.result(control.drainAndShutdown(), 1.seconds)
    val consumerSettings2 = consumerDefaults.withGroupId("consumer to producer validation")
    val receiveControl = Consumer
      .plainSource(consumerSettings2, Subscriptions.topics(targetTopic))
      .toMat(Sink.seq)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
    waitBeforeValidation()
    Await.result(receiveControl.drainAndShutdown(), 5.seconds) should have size (20)
  }

  it should "work with context" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic1 = createTopic(1)
    val topic2 = createTopic(2)
    val targetTopic = createTopic(3)
    val producerSettings = producerDefaults
    val committerSettings = committerDefaults
    // #consumerToProducerWithContext
    val control =
      Consumer
        .sourceWithOffsetContext(consumerSettings, Subscriptions.topics(topic1, topic2))
        .map { record =>
          ProducerMessage.single(new ProducerRecord(targetTopic, record.key, record.value))
        }
        .toMat(Producer.committableSinkWithOffsetContext(producerSettings, committerSettings))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
    // #consumerToProducerWithContext
    awaitProduce(produce(topic1, 1 to 10), produce(topic2, 1 to 10))
    Await.result(control.drainAndShutdown(), 1.seconds)
    val consumerSettings2 = consumerDefaults.withGroupId("consumer to producer validation")
    val receiveControl = Consumer
      .plainSource(consumerSettings2, Subscriptions.topics(targetTopic))
      .toMat(Sink.seq)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
    waitBeforeValidation()
    Await.result(receiveControl.drainAndShutdown(), 5.seconds) should have size (20)
  }

  "Backpressure per partition with batch commit" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId()).withStopTimeout(10.millis)
    val topic = createTopic()
    val maxPartitions = 100
    // #committablePartitionedSource
    val control = Consumer
      .committablePartitionedSource(consumerSettings, Subscriptions.topics(topic))
      .flatMapMerge(maxPartitions, _._2)
      .via(businessFlow)
      .map(_.committableOffset)
      .toMat(Committer.sink(committerDefaults))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
    // #committablePartitionedSource
    awaitProduce(produce(topic, 1 to 10))
    control.drainAndShutdown().futureValue should be(Done)
  }

  "Flow per partition" should "Process each assigned partition separately" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId()).withStopTimeout(10.millis)
    val comitterSettings = committerDefaults
    val topic = createTopic()
    val maxPartitions = 100
    // #committablePartitionedSource-stream-per-partition
    val control = Consumer
      .committablePartitionedSource(consumerSettings, Subscriptions.topics(topic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>
          source
            .via(businessFlow)
            .map(_.committableOffset)
            .runWith(Committer.sink(comitterSettings))
      }
      .toMat(Sink.ignore)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
    // #committablePartitionedSource-stream-per-partition
    awaitProduce(produce(topic, 1 to 10))
    control.drainAndShutdown().futureValue should be(Done)
  }

  "Rebalance Listener" should "get messages" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic()
    val assignedPromise = Promise[Done]
    val revokedPromise = Promise[Done]
    // format: off
    //#withRebalanceListenerActor
    import akka.kafka.{TopicPartitionsAssigned, TopicPartitionsRevoked}

    class RebalanceListener extends Actor with ActorLogging {
      def receive: Receive = {
        case TopicPartitionsAssigned(subscription, topicPartitions) =>
          log.info("Assigned: {}", topicPartitions)
          //#withRebalanceListenerActor
          assignedPromise.success(Done)
    //#withRebalanceListenerActor

        case TopicPartitionsRevoked(subscription, topicPartitions) =>
          log.info("Revoked: {}", topicPartitions)
          //#withRebalanceListenerActor
          revokedPromise.success(Done)
    //#withRebalanceListenerActor
      }
    }

    val rebalanceListener = system.actorOf(Props(new RebalanceListener))
    val subscription = Subscriptions
      .topics(topic)
      // additionally, pass the actor reference:
      .withRebalanceListener(rebalanceListener)

    // use the subscription as usual:
    //#withRebalanceListenerActor
    // format: on
    val (control, result) =
      //#withRebalanceListenerActor
      Consumer
        .plainSource(consumerSettings, subscription)
        //#withRebalanceListenerActor
        .toMat(Sink.seq)(Keep.both)
        .run()
    awaitProduce(produce(topic, 1 to 10))
    Await.result(control.shutdown(), 5.seconds)
    result.futureValue should have size 10
    assignedPromise.future.futureValue should be(Done)
    revokedPromise.future.futureValue should be(Done)
  }

  "Shutdown via Consumer.Control" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic()
    val offset = 123456L
    // #shutdownPlainSource
    val (consumerControl, streamComplete) =
      Consumer
        .plainSource(consumerSettings,
                     Subscriptions.assignmentWithOffset(
                       new TopicPartition(topic, 0) -> offset
                     ))
        .via(businessFlow)
        .toMat(Sink.ignore)(Keep.both)
        .run()

    consumerControl.shutdown()
    // #shutdownPlainSource
    Await.result(consumerControl.shutdown(), 5.seconds) should be(Done)
  }

  "Shutdown when batching commits" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic()
    val committerSettings = committerDefaults
    // #shutdownCommittableSource
    val drainingControl =
      Consumer
        .committableSource(consumerSettings.withStopTimeout(Duration.Zero), Subscriptions.topics(topic))
        .mapAsync(1) { msg =>
          business(msg.record).map(_ => msg.committableOffset)
        }
        .toMat(Committer.sink(committerSettings))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

    val streamComplete = drainingControl.drainAndShutdown()
    // #shutdownCommittableSource
    Await.result(streamComplete, 5.seconds) should be(Done)
  }

  "Restarting Stream" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic()
    //#restartSource
    val control = new AtomicReference[Consumer.Control](Consumer.NoopControl)

    val result = RestartSource
      .onFailuresWithBackoff(
        minBackoff = 3.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.2
      ) { () =>
        Consumer
          .plainSource(consumerSettings, Subscriptions.topics(topic))
          // this is a hack to get access to the Consumer.Control
          // instances of the latest Kafka Consumer source
          .mapMaterializedValue(c => control.set(c))
          .via(businessFlow)
      }
      .runWith(Sink.seq)

    //#restartSource
    awaitProduce(produce(topic, 1 to 10))
    //#restartSource
    control.get().shutdown()
    //#restartSource
    Await.result(result, 5.seconds) should have size 10
  }

  it should "work with committable source" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val committerSettings = committerDefaults
    val topic = createTopic()
    val partitionNumber = 0
    val control = new AtomicReference[Consumer.Control](Consumer.NoopControl)

    val result = RestartSource
      .onFailuresWithBackoff(
        minBackoff = 3.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.2
      ) { () =>
        val subscription = Subscriptions.assignment(new TopicPartition(topic, partitionNumber))
        Consumer
          .committableSource(consumerSettings, subscription)
          .mapMaterializedValue(c => control.set(c))
          .via(businessFlow)
          .map(_.committableOffset)
          .via(Committer.flow(committerSettings))
      }
      .runWith(Sink.ignore)
    awaitProduce(produce(topic, 1 to 10))
    // when shut down is desired
    val drainingControl = DrainingControl(Tuple2(control.get(), result))
    drainingControl.drainAndShutdown().futureValue shouldBe Done
  }
}

// Join flows based on automatically assigned partitions
object ConsumerWithOtherSource {
  val system = ActorSystem("example")
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val m: Materializer = ActorMaterializer.create(system)

  val maxPartitions = 100

  val config = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings =
    ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers("localhost:9092")

  def businessFlow[T] = Flow[T]

  def main(args: Array[String]): Unit = {
    // #committablePartitionedSource3
    type Msg = CommittableMessage[String, Array[Byte]]

    def zipper(left: Source[Msg, _], right: Source[Msg, _]): Source[(Msg, Msg), NotUsed] = ???

    Consumer
      .committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
      .map {
        case (topicPartition, source) =>
          // get corresponding partition from other topic
          val otherTopicPartition = new TopicPartition("otherTopic", topicPartition.partition())
          val otherSource = Consumer.committableSource(consumerSettings, Subscriptions.assignment(otherTopicPartition))
          zipper(source, otherSource)
      }
      .flatMapMerge(maxPartitions, identity)
      .via(businessFlow)
      //build commit offsets
      .batch(
        max = 20,
        seed = {
          case (left, right) =>
            (
              CommittableOffsetBatch(left.committableOffset),
              CommittableOffsetBatch(right.committableOffset)
            )
        }
      )(
        aggregate = {
          case ((batchL, batchR), (l, r)) =>
            batchL.updated(l.committableOffset)
            batchR.updated(r.committableOffset)
            (batchL, batchR)
        }
      )
      .mapAsync(1) { case (l, r) => l.commitScaladsl().map(_ => r) }
      .mapAsync(1)(_.commitScaladsl())
      .runWith(Sink.ignore)
    // #committablePartitionedSource3
  }
}
