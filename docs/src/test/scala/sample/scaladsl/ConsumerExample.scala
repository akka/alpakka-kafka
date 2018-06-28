/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package sample.scaladsl

import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.kafka._
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Flow, Keep, RestartSource, Sink, Source}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.ActorMaterializer
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import java.util.concurrent.atomic.AtomicLong

import akka.kafka.scaladsl.Consumer.DrainingControl
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import sample.DocKafkaPorts

trait ConsumerExample {
  val system = ActorSystem("example")
  implicit val ec = system.dispatcher
  implicit val m = ActorMaterializer.create(system)

  val maxPartitions = 100

  // #settings
  val config = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings =
    ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  //#settings

  val consumerSettingsWithAutoCommit =
    // #settings-autocommit
    consumerSettings
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")
  // #settings-autocommit

  val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers("localhost:9092")

  def business[T] = Flow[T]
  def businessLogic(record: ConsumerRecord[String, Array[Byte]]): Future[Done] = ???

  def terminateWhenDone(result: Future[Done]): Unit =
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
}

// Consume messages and store a representation, including offset, in DB
class ConsumerExamples extends SpecBase(DocKafkaPorts.ConsumerExamples) {
  def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort, zooKeeperPort)

  private def waitAfterProduce(): Unit = sleep(6.seconds)
  private def waitBeforeValidation(): Unit = sleep(4.seconds)

  "ExternalOffsetStorage" should "work" in {
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
        .run()
    }
    // #plainSource
    produce(topic, 1 to 10)
    sleep(2.seconds)
    Await.result(control.flatMap(_._1.shutdown()), 10.seconds) should be (Done)
    control.futureValue._2.futureValue should have size(10)
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

  "Consume messages at-most-once" should "work" in {
    val consumerSettings = consumerDefaults.withGroupId(createGroup())
    val topic = createTopic()
    // #atMostOnce
    val (control, result) =
      Consumer
        .atMostOnceSource(consumerSettings, Subscriptions.topics(topic))
        .mapAsync(1)(record => business(record.key, record.value()))
        .map(it => {
          println(s"Done with $it")
          it
        })
        .toMat(Sink.seq)(Keep.both)
        .run()
    // #atMostOnce

    produce(topic, 1 to 10)
    waitBeforeValidation()
    Await.result(control.shutdown(), 10.seconds) should be(Done)
    result.futureValue should have size (10)
  }
  // #atMostOnce

  def business(key: String, value: String): Future[Done] = // ???
    // #atMostOnce
    Future.successful(Done)

  "Consume messages at-least-once" should "work" in {
    val consumerSettings = consumerDefaults.withGroupId(createGroup())
    val topic = createTopic()
    // #atLeastOnce
    val control =
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic))
        .mapAsync(10) { msg =>
          business(msg.record.key, msg.record.value).map(_ => msg.committableOffset)
        }
        .mapAsync(5)(offset => offset.commitScaladsl())
        .toMat(Sink.seq)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
    // #atLeastOnce
    produce(topic, 1 to 10)
    waitBeforeValidation()
    Await.result(control.drainAndShutdown(), 10.seconds) should have size (10)
  }
  // format: off
  // #atLeastOnce

  def business(key: String, value: Array[Byte]): Future[Done] = ???
  // #atLeastOnce
  // format: on

  "Consume messages at-least-once, and commit in batches" should "work" in {
    val consumerSettings = consumerDefaults.withGroupId(createGroup())
    val topic = createTopic()
    // #atLeastOnceBatch
    val control =
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic))
        .mapAsync(1) { msg =>
          business(msg.record.key, msg.record.value)
            .map(_ => msg.committableOffset)
        }
        .batch(
          max = 20,
          CommittableOffsetBatch(_)
        )(_.updated(_))
        .mapAsync(3)(_.commitScaladsl())
        .toMat(Sink.seq)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
    // #atLeastOnceBatch
    produce(topic, 1 to 10)
    waitBeforeValidation()
    Await.result(control.drainAndShutdown(), 10.seconds) should have size (5)
  }

  "Connect a Consumer to Producer" should "work" in {
    val consumerSettings = consumerDefaults.withGroupId(createGroup())
    val topic1 = createTopic(1)
    val topic2 = createTopic(2)
    val targetTopic = createTopic(3)
    val producerSettings = producerDefaults
    //format: off
    // #consumerToProducerSink
    val control =
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic1, topic2))
        .map { msg =>
          ProducerMessage.Message[String, String, ConsumerMessage.CommittableOffset](
            new ProducerRecord(targetTopic, msg.record.value),
            msg.committableOffset
          )
        }
        .toMat(Producer.commitableSink(producerSettings))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
    // #consumerToProducerSink
    //format: on
    produce(topic1, 1 to 10)
    produce(topic2, 1 to 10)
    waitAfterProduce()
    Await.result(control.drainAndShutdown(), 10.seconds)
    val consumerSettings2 = consumerDefaults.withGroupId("consumer to producer validation")
    val receiveControl = Consumer
      .plainSource(consumerSettings2, Subscriptions.topics(targetTopic))
      .toMat(Sink.seq)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
    waitBeforeValidation()
    Await.result(receiveControl.drainAndShutdown(), 4.seconds) should have size (20)

  }

  "Connect a Consumer to Producer" should "support flows" in {
    val consumerSettings = consumerDefaults.withGroupId(createGroup())
    val topic = createTopic(1)
    val targetTopic = createTopic(2)
    val producerSettings = producerDefaults
    // #consumerToProducerFlow
    val control = Consumer
      .committableSource(consumerSettings, Subscriptions.topics(topic))
      .map { msg =>
        ProducerMessage.Message[String, String, ConsumerMessage.CommittableOffset](
          new ProducerRecord(targetTopic, msg.record.value),
          passThrough = msg.committableOffset
        )
      }
      .via(Producer.flexiFlow(producerSettings))
      .mapAsync(producerSettings.parallelism) { result =>
        val committable = result.passThrough
        committable.commitScaladsl()
      }
      .toMat(Sink.ignore)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
    // #consumerToProducerFlow
    produce(topic, 1 to 10)
    waitAfterProduce()
    Await.result(control.drainAndShutdown(), 10.seconds)
    val consumerSettings2 = consumerDefaults.withGroupId("consumer to producer flow validation")
    val receiveControl = Consumer
      .plainSource(consumerSettings2, Subscriptions.topics(targetTopic))
      .toMat(Sink.seq)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
    waitBeforeValidation()
    Await.result(receiveControl.drainAndShutdown(), 4.seconds) should have size (10)
  }

  "Connect a Consumer to Producer, and commit in batches" should "work" in {
    val consumerSettings = consumerDefaults.withGroupId(createGroup())
    val topic = createTopic(1)
    val targetTopic = createTopic(2)
    val producerSettings = producerDefaults
    // #consumerToProducerFlowBatch
    val control = Consumer
      .committableSource(consumerSettings, Subscriptions.topics(topic))
      .map(
        msg =>
          ProducerMessage.Message[String, String, ConsumerMessage.CommittableOffset](
            new ProducerRecord(targetTopic, msg.record.value),
            msg.committableOffset
        )
      )
      .via(Producer.flexiFlow(producerSettings))
      .map(_.passThrough)
      .batch(max = 20, CommittableOffsetBatch.apply)(_.updated(_))
      .mapAsync(3)(_.commitScaladsl())
      .toMat(Sink.ignore)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
    // #consumerToProducerFlowBatch
    produce(topic, 1 to 10)
    waitAfterProduce()
    Await.result(control.drainAndShutdown(), 10.seconds)
    val consumerSettings2 = consumerDefaults.withGroupId(createGroup())
    val receiveControl = Consumer
      .plainSource(consumerSettings2, Subscriptions.topics(targetTopic))
      .toMat(Sink.seq)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
    waitBeforeValidation()
    Await.result(receiveControl.drainAndShutdown(), 4.seconds) should have size (10)
  }

  "Connect a Consumer to Producer, and commit in batches" should "work with groupedWithin" in {
    val consumerSettings = consumerDefaults.withGroupId(createGroup())
    val topic = createTopic(1)
    val targetTopic = createTopic(2)
    val producerSettings = producerDefaults
    val source = Consumer
      .committableSource(consumerSettings, Subscriptions.topics(topic))
      .map(
        msg =>
          ProducerMessage.Message(
            new ProducerRecord[String, String](targetTopic, msg.record.value),
            msg.committableOffset
        )
      )
      .via(Producer.flexiFlow(producerSettings))
      .map(_.passThrough)
    val (control, done) =
      // #groupedWithin
      source
        .groupedWithin(10, 5.seconds)
        .map(CommittableOffsetBatch(_))
        .mapAsync(3)(_.commitScaladsl())
        // #groupedWithin
        .toMat(Sink.ignore)(Keep.both)
        .run()
    produce(topic, 1 to 10)
    waitAfterProduce()
    Await.result(control.shutdown(), 10.seconds)
    val consumerSettings2 = consumerDefaults.withGroupId(createGroup())
    val receiveControl = Consumer
      .plainSource(consumerSettings2, Subscriptions.topics(targetTopic))
      .toMat(Sink.seq)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
    waitBeforeValidation()
    Await.result(receiveControl.drainAndShutdown(), 4.seconds) should have size (10)
  }

  "Backpressure per partition with batch commit" should "work" in {
    val consumerSettings = consumerDefaults.withGroupId(createGroup())
    val topic = createTopic()
    // #committablePartitionedSource
    val (control, result) = Consumer
      .committablePartitionedSource(consumerSettings, Subscriptions.topics(topic))
      .flatMapMerge(maxPartitions, _._2)
      .via(businessFlow)
      .map(_.committableOffset)
      .batch(max = 100, CommittableOffsetBatch.apply)(_.updated(_))
      .mapAsync(3)(_.commitScaladsl())
      .toMat(Sink.seq)(Keep.both)
      .run()
    // #committablePartitionedSource
    produce(topic, 1 to 10)
    waitAfterProduce()
    Await.result(control.shutdown(), 10.seconds)
    result.futureValue should have size 4
  }

  "Flow per partition" should "Process each assigned partition separately" in {
    val consumerSettings = consumerDefaults.withGroupId(createGroup())
    val topic = createTopic()
    // #committablePartitionedSource-stream-per-partition
    val (control, result) = Consumer
      .committablePartitionedSource(consumerSettings, Subscriptions.topics(topic))
      .map {
        case (topicPartition, source) =>
          source
            .via(businessFlow)
            .mapAsync(1)(_.committableOffset.commitScaladsl())
            .runWith(Sink.ignore)
      }
      .mapAsyncUnordered(maxPartitions)(identity)
      .toMat(Sink.ignore)(Keep.both)
      .run()
    // #committablePartitionedSource-stream-per-partition
    produce(topic, 1 to 10)
    waitAfterProduce()
    Await.result(control.shutdown(), 10.seconds)
    result.futureValue should be(Done)
  }
}

// Join flows based on automatically assigned partitions
object ConsumerWithOtherSource extends ConsumerExample {
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
      .via(business)
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

//externally controlled kafka consumer
object ExternallyControlledKafkaConsumer extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #consumerActor
    //Consumer is represented by actor
    val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))

    //Manually assign topic partition to it
    val controlPartition1 = Consumer
      .plainExternalSource[String, Array[Byte]](
        consumer,
        Subscriptions.assignment(new TopicPartition("topic1", 1))
      )
      .via(business)
      .to(Sink.ignore)
      .run()

    //Manually assign another topic partition
    val controlPartition2 = Consumer
      .plainExternalSource[String, Array[Byte]](
        consumer,
        Subscriptions.assignment(new TopicPartition("topic1", 2))
      )
      .via(business)
      .to(Sink.ignore)
      .run()

    consumer ! KafkaConsumerActor.Stop
    // #consumerActor
    terminateWhenDone(controlPartition1.shutdown().flatMap(_ => controlPartition2.shutdown()))
  }
}

object ConsumerMetrics extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #consumerMetrics
    val control: Consumer.Control = Consumer
      .plainSource(consumerSettings, Subscriptions.assignment(new TopicPartition("topic1", 1)))
      .via(business)
      .to(Sink.ignore)
      .run()

    val metrics: Future[Map[MetricName, Metric]] = control.metrics
    metrics.foreach(map => println(s"metrics: ${map}"))
    // #consumerMetrics
  }
}

class RestartingStream extends ConsumerExample {

  def createStream(): Unit =
    //#restartSource
    RestartSource
      .withBackoff(
        minBackoff = 3.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.2
      ) { () =>
        Source.fromFuture {
          val source = Consumer.plainSource(consumerSettings, Subscriptions.topics("topic1"))
          source
            .via(business)
            .watchTermination() {
              case (consumerControl, futureDone) =>
                futureDone
                  .flatMap { _ =>
                    consumerControl.shutdown()
                  }
                  .recoverWith { case _ => consumerControl.shutdown() }
            }
            .runWith(Sink.ignore)
        }
      }
      .runWith(Sink.ignore)
  //#restartSource
}

object RebalanceListenerExample extends ConsumerExample {
  //#withRebalanceListenerActor
  import akka.kafka.TopicPartitionsAssigned
  import akka.kafka.TopicPartitionsRevoked

  class RebalanceListener extends Actor with ActorLogging {
    def receive: Receive = {
      case TopicPartitionsAssigned(sub, assigned) ⇒
        log.info("Assigned: {}", assigned)

      case TopicPartitionsRevoked(sub, revoked) ⇒
        log.info("Revoked: {}", revoked)
    }
  }

  //#withRebalanceListenerActor

  def createActor(implicit system: ActorSystem): Source[ConsumerRecord[String, Array[Byte]], Consumer.Control] = {
    //#withRebalanceListenerActor
    val rebalanceListener = system.actorOf(Props[RebalanceListener])

    val subscription = Subscriptions
      .topics(Set("topic"))
      // additionally, pass the actor reference:
      .withRebalanceListener(rebalanceListener)

    // use the subscription as usual:
    Consumer
      .plainSource(consumerSettings, subscription)
    //#withRebalanceListenerActor
  }

}

// Shutdown via Consumer.Control
object ShutdownPlainSourceExample extends ConsumerExample {

  def main(args: Array[String]): Unit = {
    val offset = 123456L
    // #shutdownPlainSource
    val (consumerControl, streamComplete) =
      Consumer
        .plainSource(consumerSettings,
                     Subscriptions.assignmentWithOffset(
                       new TopicPartition("topic1", 0) -> offset
                     ))
        .mapAsync(1)(businessLogic)
        .toMat(Sink.ignore)(Keep.both)
        .run()

    consumerControl.shutdown()
    // #shutdownPlainSource
    terminateWhenDone(streamComplete)
  }

}

// Shutdown when batching commits
object ShutdownCommitableSourceExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #shutdownCommitableSource
    val drainingControl =
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .mapAsync(1) { msg =>
          businessLogic(msg.record).map(_ => msg.committableOffset)
        }
        .batch(max = 20, first => CommittableOffsetBatch(first)) { (batch, elem) =>
          batch.updated(elem)
        }
        .mapAsync(3)(_.commitScaladsl())
        .toMat(Sink.ignore)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

    val streamComplete = drainingControl.drainAndShutdown()

    // #shutdownCommitableSource

    terminateWhenDone(streamComplete)
  }
}
