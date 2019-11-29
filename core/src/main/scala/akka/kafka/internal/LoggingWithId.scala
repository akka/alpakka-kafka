/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.{Actor, ActorLogging}
import akka.event.LoggingAdapter
import akka.stream.stage.{GraphStageLogic, StageLogging}

/**
 * Generate a short random UID for something.
 */
private[internal] trait InstanceId {
  val id: String = java.util.UUID.randomUUID().toString.take(5)
}

/**
 * Override akka streams [[StageLogging]] to include an ID from [[InstanceId]] as a prefix to each logging statement.
 */
private[internal] trait StageIdLogging extends StageLogging with InstanceId { self: GraphStageLogic =>
  private[this] var _log: LoggingAdapter = _
  override def log: LoggingAdapter = {
    if (_log eq null) {
      _log = new LoggingAdapterWithId(super.log, id)
    }
    _log
  }
}

/**
 * Override akka classic [[ActorLogging]] to include an ID from [[InstanceId]] as a prefix to each logging statement.
 */
private[internal] trait ActorIdLogging extends ActorLogging with InstanceId { this: Actor =>
  private[this] var _log: LoggingAdapter = _
  override def log: LoggingAdapter = {
    if (_log eq null) {
      _log = new LoggingAdapterWithId(super.log, id)
    }
    _log
  }
}

private[internal] final class LoggingAdapterWithId(logger: LoggingAdapter, id: String) extends LoggingAdapter {
  private val idPrefix: String = s"[$id] "
  private def msgWithId(message: String): String = idPrefix + message

  override protected def notifyError(message: String): Unit = logger.error(msgWithId(message))
  override protected def notifyError(cause: Throwable, message: String): Unit = logger.error(msgWithId(message), cause)
  override protected def notifyWarning(message: String): Unit = logger.warning(msgWithId(message))
  override protected def notifyInfo(message: String): Unit = logger.info(msgWithId(message))
  override protected def notifyDebug(message: String): Unit = logger.debug(msgWithId(message))

  override def isErrorEnabled: Boolean = logger.isErrorEnabled
  override def isWarningEnabled: Boolean = logger.isWarningEnabled
  override def isInfoEnabled: Boolean = logger.isInfoEnabled
  override def isDebugEnabled: Boolean = logger.isDebugEnabled
}
