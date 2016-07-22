/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package examples.scaladsl

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}

import scala.concurrent.Future
import scala.concurrent.duration._

trait ConsumerExample {
  implicit val system = ActorSystem("example")
  implicit val ec = system.dispatcher
  implicit val m = ActorMaterializer(ActorMaterializerSettings(system))

  val maxPartitions = 100
  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")

  def business[T] = Flow[T]

  class DB {
    def save(record: ConsumerRecord[Array[Byte], String]): Future[Done] = ???

    def loadOffset(): Future[Long] = ???

    def update(data: String): Future[Done] = ???
  }

  class Rocket {
    def launch(destination: String): Future[Done] = ???
  }
}

// Consume messages and store a representation, including offset, in DB
object ExternalOffsetStorageExample extends ConsumerExample {
  val db = new DB
  db.loadOffset().foreach { fromOffset =>
    val subscription = Subscriptions.assignmentWithOffset(new TopicPartition("topic1", 1) -> fromOffset)
    Consumer.plainSource(consumerSettings, subscription)
      .mapAsync(1)(db.save)
  }
}

// Consume messages at-most-once
object AtMostOnceExample extends ConsumerExample {
  val rocket = new Rocket

  Consumer.atMostOnceSource(consumerSettings.withClientId("client1"), Subscriptions.topics("topic1"))
    .mapAsync(1) { record =>
      rocket.launch(record.value)
    }
}

// Consume messages at-least-once
object AtLeastOnceExample extends ConsumerExample {
  val db = new DB

  Consumer.committableSource(consumerSettings.withClientId("client1"), Subscriptions.topics("topic1"))
    .mapAsync(1) { msg =>
      db.update(msg.value).flatMap(_ => msg.committableOffset.commitScaladsl())
    }
}

// Consume messages at-least-once, and commit in batches
object AtLeastOnceWithBatchCommitExample extends ConsumerExample {
  val db = new DB

  Consumer.committableSource(consumerSettings.withClientId("client1"), Subscriptions.topics("topic1"))
    .mapAsync(1) { msg =>
      db.update(msg.value).map(_ => msg.committableOffset)
    }
    .batch(max = 10, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
      batch.updated(elem)
    }
    .mapAsync(1)(_.commitScaladsl())
}

// Connect a Consumer to Producer
object ConsumerToProducerSinkExample extends ConsumerExample {
  Consumer.committableSource(consumerSettings.withClientId("client1"), Subscriptions.topics("topic1"))
    .map(msg =>
      ProducerMessage.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset))
    .to(Producer.commitableSink(producerSettings))
}

// Connect a Consumer to Producer
object ConsumerToProducerFlowExample extends ConsumerExample {
  Consumer.committableSource(consumerSettings.withClientId("client1"), Subscriptions.topics("topic1"))
    .map(msg =>
      ProducerMessage.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset))
    .via(Producer.flow(producerSettings))
    .mapAsync(producerSettings.parallelism) { result => result.message.passThrough.commitScaladsl() }
}

// Connect a Consumer to Producer, and commit in batches
object ConsumerToProducerWithBatchCommitsExample extends ConsumerExample {
  Consumer.committableSource(consumerSettings.withClientId("client1"), Subscriptions.topics("topic1"))
    .map(msg =>
      ProducerMessage.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset))
    .via(Producer.flow(producerSettings))
    .map(_.message.passThrough)
    .batch(max = 10, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
      batch.updated(elem)
    }
    .mapAsync(producerSettings.parallelism)(_.commitScaladsl())
}

// Connect a Consumer to Producer, and commit in batches
object ConsumerToProducerWithBatchCommits2Example extends ConsumerExample {
  Consumer.committableSource(consumerSettings.withClientId("client1"), Subscriptions.topics("topic1"))
    .map(msg =>
      ProducerMessage.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset))
    .via(Producer.flow(producerSettings))
    .map(_.message.passThrough)
    .groupedWithin(10, 5.seconds)
    .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem) })
    .mapAsync(producerSettings.parallelism)(_.commitScaladsl())
}

// Backpressure per partition with batch commit
object ConsumerWithPerPartitionBackpressure extends ConsumerExample {
  val control = Consumer.committablePartitionedSource(consumerSettings.withClientId("client1"), Subscriptions.topics("topic1"))
    .flatMapMerge(maxPartitions, _._2)
    .via(business)
    .batch(max = 100, first => CommittableOffsetBatch.empty.updated(first.committableOffset)) { (batch, elem) =>
      batch.updated(elem.committableOffset)
    }
    .mapAsync(1)(_.commitScaladsl())
}

//externally controlled kafka consumer
object ExternallyControlledKafkaConsumer extends ConsumerExample {
  //Consumer is represented by actor
  //Create new consumer
  val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))

  //Manually assign topic partition to it
  val stream1 = Consumer
    .plainExternalSource[Array[Byte], String](consumer, Subscriptions.assignment(new TopicPartition("topic1", 1)))
    .via(business)
    .to(Sink.ignore)

  //Manually assign another topic partition
  val stream2 = Consumer
    .plainExternalSource[Array[Byte], String](consumer, Subscriptions.assignment(new TopicPartition("topic1", 2)))
    .via(business)
    .to(Sink.ignore)
}

// Flow per partition
object ConsumerWithIndependentFlowsPerPartition extends ConsumerExample {
  //Consumer group represented as Source[(TopicPartition, Source[Messages])]
  val consumerGroup = Consumer.committablePartitionedSource(consumerSettings.withClientId("client1"), Subscriptions.topics("topic1"))
  //Process each assigned partition separately
  consumerGroup.map {
    case (topicPartition, source) =>
      source
        .via(business)
        .toMat(Sink.ignore)(Keep.both)
        .run()
  }
    .mapAsyncUnordered(maxPartitions)(_._2)
}

// Join flows based on automatically assigned partitions
object ConsumerWithOtherSource extends ConsumerExample {
  type Msg = CommittableMessage[Array[Byte], String]
  def zipper(left: Source[Msg, _], right: Source[Msg, _]): Source[(Msg, Msg), NotUsed] = ???

  val control = Consumer.committablePartitionedSource(consumerSettings.withClientId("client1"), Subscriptions.topics("topic1"))
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
    .batch(max = 100, {
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
}
