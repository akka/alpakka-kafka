/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package examples

import scala.concurrent.duration._
import scala.concurrent.Future
import akka.Done
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import akka.kafka.scaladsl.Consumer.CommittableOffsetBatch
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Producer
import org.apache.kafka.clients.consumer.ConsumerRecord
import akka.kafka.ConsumerSettings
import akka.kafka.ProducerSettings
import org.apache.kafka.common.TopicPartition

trait ConsumerExample {
  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  def consumerSettings: ConsumerSettings[Array[Byte], String] = ???

  def producerSettings: ProducerSettings[Array[Byte], String] = ???

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
    val settings = consumerSettings
      .withFromOffset(new TopicPartition("topic1", 1), fromOffset)
    Consumer.plainSource(settings)
      .mapAsync(1)(db.save)
  }
}

// Consume messages at-most-once
object AtMostOnceExample extends ConsumerExample {
  val rocket = new Rocket

  Consumer.atMostOnceSource(consumerSettings.withClientId("client1"))
    .mapAsync(1) { record =>
      rocket.launch(record.value)
    }
}

// Consume messages at-least-once
object AtLeastOnceExample extends ConsumerExample {
  val db = new DB

  Consumer.committableSource(consumerSettings.withClientId("client1"))
    .mapAsync(1) { msg =>
      db.update(msg.value).flatMap(_ => msg.committableOffset.commit())
    }
}

// Consume messages at-least-once, and commit in batches
object AtLeastOnceWithBatchCommitExample extends ConsumerExample {
  val db = new DB

  Consumer.committableSource(consumerSettings.withClientId("client1"))
    .mapAsync(1) { msg =>
      db.update(msg.value).map(_ => msg.committableOffset)
    }
    .batch(max = 10, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
      batch.updated(elem)
    }
    .mapAsync(1)(_.commit())
}

// Connect a Consumer to Producer
object ConsumerToProducerSinkExample extends ConsumerExample {
  Consumer.committableSource(consumerSettings.withClientId("client1"))
    .map(msg =>
      Producer.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset))
    .to(Producer.commitableSink(producerSettings))
}

// Connect a Consumer to Producer
object ConsumerToProducerFlowExample extends ConsumerExample {
  Consumer.committableSource(consumerSettings.withClientId("client1"))
    .map(msg =>
      Producer.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset))
    .via(Producer.flow(producerSettings))
    .mapAsync(producerSettings.parallelism) { result => result.message.passThrough.commit() }
}

// Connect a Consumer to Producer, and commit in batches
object ConsumerToProducerWithBatchCommitsExample extends ConsumerExample {
  Consumer.committableSource(consumerSettings.withClientId("client1"))
    .map(msg =>
      Producer.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset))
    .via(Producer.flow(producerSettings))
    .map(_.message.passThrough)
    .batch(max = 10, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
      batch.updated(elem)
    }
    .mapAsync(producerSettings.parallelism)(_.commit())
}

// Connect a Consumer to Producer, and commit in batches
object ConsumerToProducerWithBatchCommits2Example extends ConsumerExample {
  Consumer.committableSource(consumerSettings.withClientId("client1"))
    .map(msg =>
      Producer.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset))
    .via(Producer.flow(producerSettings))
    .map(_.message.passThrough)
    .groupedWithin(10, 5.seconds)
    .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem) })
    .mapAsync(producerSettings.parallelism)(_.commit())
}

