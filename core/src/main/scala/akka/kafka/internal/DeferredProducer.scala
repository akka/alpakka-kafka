/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.kafka.ProducerSettings
import akka.stream.stage._
import akka.util.JavaDurationConverters._
import org.apache.kafka.clients.producer.Producer

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] trait DeferredProducer[K, V] {
  self: GraphStageLogic with StageIdLogging =>

  /** The Kafka producer may be created lazily, assigned via `preStart` in `assignProducer`. */
  protected var producer: Producer[K, V] = _

  protected def producerSettings: ProducerSettings[K, V]
  protected def producerAssigned(): Unit
  protected def closeAndFailStageCb: AsyncCallback[Throwable]

  private def assignProducer(p: Producer[K, V]): Unit = {
    producer = p
    producerAssigned()
  }

  final protected def resolveProducer(): Unit = {
    val producerFuture = producerSettings.createKafkaProducerAsync()(materializer.executionContext)
    producerFuture.value match {
      case Some(Success(p)) => assignProducer(p)
      case Some(Failure(e)) => failStage(e)
      case None =>
        val assign = getAsyncCallback(assignProducer)
        producerFuture
          .transform(
            producer => assign.invoke(producer),
            e => {
              log.error(e, "producer creation failed")
              closeAndFailStageCb.invoke(e)
              e
            }
          )(ExecutionContexts.sameThreadExecutionContext)
    }
  }

  protected def closeProducerImmediately(): Unit =
    if (producer != null) {
      // Discard unsent ProducerRecords after encountering a send-failure in ProducerStage
      // https://github.com/akka/alpakka-kafka/pull/318
      producer.close(java.time.Duration.ZERO)
    }

  protected def closeProducer(): Unit =
    if (producerSettings.closeProducerOnStop && producer != null) {
      try {
        // we do not have to check if producer was already closed in send-callback as `flush()` and `close()` are effectively no-ops in this case
        producer.flush()
        producer.close(producerSettings.closeTimeout.asJava)
        log.debug("Producer closed")
      } catch {
        case NonFatal(ex) => log.error(ex, "Problem occurred during producer close")
      }
    }

}
