/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package sample.scaladsl

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.kafka._
import akka.actor.{Props, ActorRef, Actor, ActorSystem, ActorLogging, PoisonPill}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.ActorMaterializer
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import java.util.concurrent.atomic.AtomicLong

trait ConsumerExample {
  val system = ActorSystem("example")
  implicit val ec = system.dispatcher
  implicit val m = ActorMaterializer.create(system)

  val maxPartitions = 100

  // #settings
  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  //#settings

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")

  def business[T] = Flow[T]

  // #db
  class DB {

    private val offset = new AtomicLong

    def save(record: ConsumerRecord[Array[Byte], String]): Future[Done] = {
      println(s"DB.save: ${record.value}")
      offset.set(record.offset)
      Future.successful(Done)
    }

    def loadOffset(): Future[Long] =
      Future.successful(offset.get)

    def update(data: String): Future[Done] = {
      println(s"DB.update: $data")
      Future.successful(Done)
    }
  }
  // #db

  // #rocket
  class Rocket {
    def launch(destination: String): Future[Done] = {
      println(s"Rocket launched to $destination")
      Future.successful(Done)
    }
  }
  // #rocket

  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
  }
}

// Consume messages and store a representation, including offset, in DB
object ExternalOffsetStorageExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #plainSource
    val db = new DB
    db.loadOffset().foreach { fromOffset =>
      val partition = 0
      val subscription = Subscriptions.assignmentWithOffset(
        new TopicPartition("topic1", partition) -> fromOffset
      )
      val done =
        Consumer.plainSource(consumerSettings, subscription)
          .mapAsync(1)(db.save)
          .runWith(Sink.ignore)
      // #plainSource

      terminateWhenDone(done)
    }
  }
}

// Consume messages at-most-once
object AtMostOnceExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #atMostOnce
    val rocket = new Rocket

    val done = Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("topic1"))
      .mapAsync(1) { record =>
        rocket.launch(record.value)
      }
      .runWith(Sink.ignore)
    // #atMostOnce

    terminateWhenDone(done)
  }
}

// Consume messages at-least-once
object AtLeastOnceExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #atLeastOnce
    val db = new DB

    val done =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .mapAsync(1) { msg =>
          db.update(msg.record.value).map(_ => msg)
        }
        .mapAsync(1) { msg =>
          msg.committableOffset.commitScaladsl()
        }
        .runWith(Sink.ignore)
    // #atLeastOnce

    terminateWhenDone(done)
  }
}

// Consume messages at-least-once, and commit in batches
object AtLeastOnceWithBatchCommitExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #atLeastOnceBatch
    val db = new DB

    val done =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .mapAsync(1) { msg =>
          db.update(msg.record.value).map(_ => msg.committableOffset)
        }
        .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
          batch.updated(elem)
        }
        .mapAsync(3)(_.commitScaladsl())
        .runWith(Sink.ignore)
    // #atLeastOnceBatch

    terminateWhenDone(done)
  }
}

// Connect a Consumer to Producer
object ConsumerToProducerSinkExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #consumerToProducerSink
    Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .map { msg =>
        println(s"topic1 -> topic2: $msg")
        ProducerMessage.Message(new ProducerRecord[Array[Byte], String](
          "topic2",
          msg.record.value
        ), msg.committableOffset)
      }
      .runWith(Producer.commitableSink(producerSettings))
    // #consumerToProducerSink
  }
}

// Connect a Consumer to Producer
object ConsumerToProducerFlowExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #consumerToProducerFlow
    val done =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .map { msg =>
          println(s"topic1 -> topic2: $msg")
          ProducerMessage.Message(new ProducerRecord[Array[Byte], String](
            "topic2",
            msg.record.value
          ), msg.committableOffset)
        }
        .via(Producer.flow(producerSettings))
        .mapAsync(producerSettings.parallelism) { result =>
          result.message.passThrough.commitScaladsl()
        }
        .runWith(Sink.ignore)
    // #consumerToProducerFlow

    terminateWhenDone(done)
  }
}

// Connect a Consumer to Producer, and commit in batches
object ConsumerToProducerWithBatchCommitsExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #consumerToProducerFlowBatch
    val done =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .map(msg =>
          ProducerMessage.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.record.value), msg.committableOffset))
        .via(Producer.flow(producerSettings))
        .map(_.message.passThrough)
        .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
          batch.updated(elem)
        }
        .mapAsync(3)(_.commitScaladsl())
        .runWith(Sink.ignore)
    // #consumerToProducerFlowBatch

    terminateWhenDone(done)
  }
}

// Connect a Consumer to Producer, and commit in batches
object ConsumerToProducerWithBatchCommits2Example extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    val done =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .map(msg =>
          ProducerMessage.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.record.value), msg.committableOffset))
        .via(Producer.flow(producerSettings))
        .map(_.message.passThrough)
        // #groupedWithin
        .groupedWithin(10, 5.seconds)
        .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem) })
        .mapAsync(3)(_.commitScaladsl())
        // #groupedWithin
        .runWith(Sink.ignore)

    terminateWhenDone(done)
  }
}

// Backpressure per partition with batch commit
object ConsumerWithPerPartitionBackpressure extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #committablePartitionedSource
    val done = Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
      .flatMapMerge(maxPartitions, _._2)
      .via(business)
      .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first.committableOffset)) { (batch, elem) =>
        batch.updated(elem.committableOffset)
      }
      .mapAsync(3)(_.commitScaladsl())
      .runWith(Sink.ignore)
    // #committablePartitionedSource

    terminateWhenDone(done)
  }
}

// Flow per partition
object ConsumerWithIndependentFlowsPerPartition extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #committablePartitionedSource2
    //Consumer group represented as Source[(TopicPartition, Source[Messages])]
    val consumerGroup =
      Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
    //Process each assigned partition separately
    consumerGroup.map {
      case (topicPartition, source) =>
        source
          .via(business)
          .toMat(Sink.ignore)(Keep.both)
          .run()
    }
      .mapAsyncUnordered(maxPartitions)(_._2)
      .runWith(Sink.ignore)
    // #committablePartitionedSource2
  }
}

// Join flows based on automatically assigned partitions
object ConsumerWithOtherSource extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #committablePartitionedSource3
    type Msg = CommittableMessage[Array[Byte], String]
    def zipper(left: Source[Msg, _], right: Source[Msg, _]): Source[(Msg, Msg), NotUsed] = ???

    Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
      .map {
        case (topicPartition, source) =>
          // get corresponding partition from other topic
          val otherSource = {
            val otherTopicPartition = new TopicPartition("otherTopic", topicPartition.partition())
            Consumer.committableSource(consumerSettings, Subscriptions.assignment(otherTopicPartition))
          }
          zipper(source, otherSource)
      }
      .flatMapMerge(maxPartitions, identity)
      .via(business)
      //build commit offsets
      .batch(max = 20, {
        case (l, r) => (
          CommittableOffsetBatch.empty.updated(l.committableOffset),
          CommittableOffsetBatch.empty.updated(r.committableOffset)
        )
      }) {
        case ((batchL, batchR), (l, r)) =>
          batchL.updated(l.committableOffset)
          batchR.updated(r.committableOffset)
          (batchL, batchR)
      }
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
    Consumer
      .plainExternalSource[Array[Byte], String](consumer, Subscriptions.assignment(new TopicPartition("topic1", 1)))
      .via(business)
      .runWith(Sink.ignore)

    //Manually assign another topic partition
    Consumer
      .plainExternalSource[Array[Byte], String](consumer, Subscriptions.assignment(new TopicPartition("topic1", 2)))
      .via(business)
      .runWith(Sink.ignore)
    // #consumerActor
  }
}

class StreamWrapperActor extends Actor with ConsumerExample with ActorLogging {

  def receive = {
    case _ =>
  }

  def processMsg(g: ConsumerRecord[Array[Byte], String]): Future[String] = Future.successful("")

  def createStream(): Unit = {
    //#errorHandlingStop
    val done =
      Consumer.plainSource(consumerSettings, Subscriptions.topics("topic1"))
        .mapAsync(1)(processMsg)
        .runWith(Sink.ignore)

    done.onComplete {
      case Failure(ex) =>
        log.error(ex, "Stream failed, stopping the actor.")
        self ! PoisonPill
      case Success(ex) => // gracceful stream shutdown handling
    }
    //#errorHandlingStop
  }

}

object StreamWrapperActor {

  def create(implicit system: ActorSystem): ActorRef = {
    //#errorHandlingSupervisor
    import akka.pattern.{Backoff, BackoffSupervisor}

    val childProps = Props(classOf[StreamWrapperActor])

    val supervisorProps = BackoffSupervisor.props(
      Backoff.onStop(
        childProps,
        childName = "streamActor",
        minBackoff = 3.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.2
      )
    )
    val supervisor = system.actorOf(supervisorProps, name = "streamActorSupervisor")
    //#errorHandlingSupervisor
    supervisor
  }
}
