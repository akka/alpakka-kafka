/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.{Actor, ActorLogging, Props, Timers}
import akka.annotation.InternalApi
import akka.event.LoggingReceive
import akka.kafka.{ConnectionCheckerSettings, KafkaConnectionFailed, Metadata}
import org.apache.kafka.common.errors.TimeoutException

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

@InternalApi private class ConnectionChecker(config: ConnectionCheckerSettings)
    extends Actor
    with ActorLogging
    with Timers {

  import ConnectionChecker.Internal._
  import config._

  override def preStart(): Unit = {
    super.preStart()
    startRegularTimer()
  }

  override def receive: Receive = regular

  def regular: Receive =
    LoggingReceive.withLabel("regular")(behaviour(0, checkInterval))

  def backoff(failedAttempts: Int = 1, backoffCheckInterval: FiniteDuration): Receive =
    LoggingReceive.withLabel(s"backoff($failedAttempts, $backoffCheckInterval)")(
      behaviour(failedAttempts, backoffCheckInterval)
    )

  def behaviour(failedAttempts: Int, interval: FiniteDuration): Receive = {
    case CheckConnection =>
      context.parent ! Metadata.ListTopics

    case Metadata.Topics(Failure(te: TimeoutException)) =>
      //failedAttempts is a sum of first triggered failure and retries (retries + 1)
      if (failedAttempts == maxRetries) {
        context.parent ! KafkaConnectionFailed(te, maxRetries)
        context.stop(self)
      } else context.become(backoff(failedAttempts + 1, startBackoffTimer(interval)))

    case Metadata.Topics(Success(_)) =>
      startRegularTimer()
      context.become(regular)
  }

  def startRegularTimer(): Unit = timers.startSingleTimer(RegularCheck, CheckConnection, checkInterval)

  /** start single timer and return it's interval
   *
   * @param previousInterval previous CheckConnection interval
   * @return new backoff interval (previousInterval * factor)
   */
  def startBackoffTimer(previousInterval: FiniteDuration): FiniteDuration = {
    val backoffCheckInterval = (previousInterval * factor).asInstanceOf[FiniteDuration]
    timers.startSingleTimer(BackoffCheck, CheckConnection, backoffCheckInterval)
    backoffCheckInterval
  }

}

@InternalApi object ConnectionChecker {

  def props(config: ConnectionCheckerSettings): Props = Props(new ConnectionChecker(config))

  private object Internal {
    //Timer labels
    case object RegularCheck
    case object BackoffCheck

    //Commands
    case object CheckConnection
  }

}
