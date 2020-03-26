/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.ProducerMessage._
import akka.kafka.ProducerSettings
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}

import scala.concurrent.{ExecutionContext, Future, Promise}
import akka.util.JavaDurationConverters._

/**
 * Utility class for producing to Kafka without using Akka Streams.
 * @param settings producer settings used to create or access the [[org.apache.kafka.clients.producer.Producer]]
 */
final class ElementProducer[K, V] private (val settings: ProducerSettings[K, V])(implicit ec: ExecutionContext)
    extends AutoCloseable {
  private final val producerFuture = settings.createKafkaProducerAsync()(ec)

  /**
   * Send records to Kafka topics and complete a future with the result.
   *
   * It publishes records to Kafka topics conditionally:
   *
   * - [[akka.kafka.ProducerMessage.Message Message]] publishes a single message to its topic, and completes the future with [[akka.kafka.ProducerMessage.Result Result]]
   *
   * - [[akka.kafka.ProducerMessage.MultiMessage MultiMessage]] publishes all messages in its `records` field, and completes the future with [[akka.kafka.ProducerMessage.MultiResult MultiResult]]
   *
   * - [[akka.kafka.ProducerMessage.PassThroughMessage PassThroughMessage]] does not publish anything, and completes the future with [[akka.kafka.ProducerMessage.PassThroughResult PassThroughResult]]
   *
   * The messages support passing through arbitrary data.
   */
  def sendEnvelope[PT](envelope: Envelope[K, V, PT]): Future[Results[K, V, PT]] = {
    producerFuture.flatMap { producer =>
      envelope match {
        case msg: Message[K, V, PT] =>
          sendSingle(producer, msg.record, Result(_, msg))

        case multiMsg: MultiMessage[K, V, PT] =>
          val promises = multiMsg.records.map(record => sendSingle(producer, record, MultiResultPart(_, record)))
          Future.sequence(promises).map(MultiResult(_, multiMsg.passThrough))

        case passThrough: PassThroughMessage[K, V, PT] =>
          Future.successful(PassThroughResult(passThrough.passThrough))

      }
    }
  }

  /**
   * Send records to Kafka topics and complete a future with the result.
   *
   * The message supports passing through arbitrary data.
   */
  def sendMessage[PT](message: Message[K, V, PT]): Future[Result[K, V, PT]] = {
    producerFuture.flatMap { producer =>
      sendSingle(producer, message.record, meta => Result(meta, message))
    }
  }

  /**
   * Send a raw Kafka [[org.apache.kafka.clients.producer.ProducerRecord]] and complete a future with the resulting metadata.
   */
  def send(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    producerFuture.flatMap { producer =>
      sendSingle(producer, record, identity)
    }
  }

  private def sendSingle[R](producer: Producer[K, V],
                            record: ProducerRecord[K, V],
                            success: RecordMetadata => R): Future[R] = {
    val result = Promise[R]
    producer.send(
      record,
      new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null)
            result.success(success(metadata))
          else
            result.failure(exception)
        }
      }
    )
    result.future
  }

  /**
   * Close the underlying producer (depending on the "close producer on stop" setting).
   */
  def close(): Unit = {
    if (settings.closeProducerOnStop) producerFuture.foreach { producer =>
      // we do not have to check if producer was already closed in send-callback as `flush()` and `close()` are effectively no-ops in this case
      producer.flush()
      producer.close(settings.closeTimeout.asJava)
    }
  }

  override def toString: String = s"ElementProducer($settings)"
}

object ElementProducer {
  def apply[K, V](settings: ProducerSettings[K, V])(implicit ec: ExecutionContext): ElementProducer[K, V] =
    new ElementProducer(settings)
}
