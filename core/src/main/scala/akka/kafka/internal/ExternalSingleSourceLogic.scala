/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.ActorRef
import akka.kafka.ManualSubscription
import akka.stream.SourceShape

import scala.concurrent.Future

private[kafka] abstract class ExternalSingleSourceLogic[K, V, Msg](
    shape: SourceShape[Msg],
    _consumerActor: ActorRef,
    subscription: ManualSubscription
) extends BaseSingleSourceLogic[K, V, Msg](shape) {

  final override protected def logSource: Class[_] = classOf[ExternalSingleSourceLogic[K, V, Msg]]

  final val consumerFuture: Future[ActorRef] = Future.successful(_consumerActor)

  final def createConsumerActor(): ActorRef = _consumerActor

  final def configureSubscription(): Unit =
    configureManualSubscription(subscription)

  final def performShutdown(): Unit =
    completeStage()

}
