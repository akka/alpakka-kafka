/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.annotation.InternalApi
import akka.kafka.ProducerSettings
import akka.stream.Materializer
import akka.stream.stage._
import org.apache.kafka.clients.producer.Producer

import scala.concurrent.ExecutionContext
import scala.jdk.DurationConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] object DeferredProducer {

  /**
   * The [[ProducerAssignmentLifecycle]] allows us to track the state of the asynchronous producer assignment
   * within the stage. This is useful when we need to manage different behavior during the assignment process. For
   * example, in [[TransactionalProducerStageLogic]] we match on the lifecycle when extracting the transactional.id
   * of the first message received from a partitioned source.
   */
  sealed trait ProducerAssignmentLifecycle
  case object Unassigned extends ProducerAssignmentLifecycle
  case object AsyncCreateRequestSent extends ProducerAssignmentLifecycle
  case object Assigned extends ProducerAssignmentLifecycle
}

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] trait DeferredProducer[K, V] extends GraphStageLogic with StageIdLogging {

  import DeferredProducer._

  /** The Kafka producer may be created lazily, assigned via `preStart` in `assignProducer`. */
  protected var producer: Producer[K, V] = _
  protected var producerAssignmentLifecycle: ProducerAssignmentLifecycle = Unassigned

  protected def producerSettings: ProducerSettings[K, V]
  protected def producerAssigned(): Unit
  protected def closeAndFailStageCb: AsyncCallback[Throwable]

  private def assignProducer(p: Producer[K, V]): Unit = {
    producer = p
    changeProducerAssignmentLifecycle(Assigned)
    producerAssigned()
  }

  final protected def resolveProducer(settings: ProducerSettings[K, V]): Unit = {
    val producerFuture = settings.createKafkaProducerAsync()(materializer.executionContext)
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
          )(ExecutionContext.parasitic)
        changeProducerAssignmentLifecycle(AsyncCreateRequestSent)
    }
  }

  private def changeProducerAssignmentLifecycle(state: ProducerAssignmentLifecycle): Unit = {
    val oldState = producerAssignmentLifecycle
    producerAssignmentLifecycle = state
    log.debug("Asynchronous producer assignment lifecycle changed '{} -> {}'", oldState, state)
  }

  protected def closeProducerImmediately(): Unit =
    if (producer != null && producerSettings.closeProducerOnStop) {
      // Discard unsent ProducerRecords after encountering a send-failure in ProducerStage
      // https://github.com/akka/alpakka-kafka/pull/318
      producer.close(java.time.Duration.ZERO)
    }

  protected def closeProducer(): Unit =
    if (producerSettings.closeProducerOnStop && producerAssignmentLifecycle == Assigned) {
      try {
        // we do not have to check if producer was already closed in send-callback as `flush()` and `close()` are effectively no-ops in this case
        producer.flush()
        producer.close(producerSettings.closeTimeout.toJava)
        log.debug("Producer closed")
      } catch {
        case NonFatal(ex) => log.error(ex, "Problem occurred during producer close")
      }
    }

}
