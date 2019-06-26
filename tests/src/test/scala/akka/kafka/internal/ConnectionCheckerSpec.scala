/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.Metadata
import akka.kafka.EnabledConnectionCheckerSettings
import akka.kafka.KafkaConnectionFailed
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.errors.TimeoutException
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class ConnectionCheckerSpec
    extends TestKit(ActorSystem("KafkaConnectionCheckerSpec", ConfigFactory.load()))
    with WordSpecLike
    with Matchers {

  "KafkaConnectionChecker" must {

    val retryInterval = 100.millis
    implicit val config: EnabledConnectionCheckerSettings =
      EnabledConnectionCheckerSettings(3, retryInterval, 2d)

    "wait for response and retryInterval before perform new ask" in withCheckerActorRef { checker =>
      expectListTopicsRequest(retryInterval)

      Thread.sleep(retryInterval.toMillis)
      checker ! Metadata.Topics(Success(Map.empty))

      expectListTopicsRequest(retryInterval)
    }

    "exponentially retry on failure and failed after max retries exceeded" in withCheckerActorRef { checker =>
      var interval = retryInterval
      for (_ <- 1 to (config.maxRetries + 1)) {
        expectListTopicsRequest(interval)
        checker ! Metadata.Topics(Failure(new TimeoutException()))
        interval = newExponentialInterval(interval, config.factor)
      }

      watch(checker)
      expectMsgType[KafkaConnectionFailed]
      expectTerminated(checker)
    }

    "return to normal mode if in backoff mode receive Metadata.Topics(success)" in withCheckerActorRef { checker =>
      expectListTopicsRequest(retryInterval)
      checker ! Metadata.Topics(Failure(new TimeoutException()))

      expectListTopicsRequest(newExponentialInterval(retryInterval, config.factor))
      checker ! Metadata.Topics(Success(Map.empty))

      expectListTopicsRequest(retryInterval)
    }
  }

  def newExponentialInterval(previousInterval: FiniteDuration, factor: Double): FiniteDuration =
    (previousInterval * factor).asInstanceOf[FiniteDuration]

  def expectListTopicsRequest(interval: FiniteDuration): Unit = {
    expectNoMessage(interval - 20.millis)
    expectMsg(Metadata.ListTopics)
  }

  def withCheckerActorRef[T](block: ActorRef => T)(implicit config: EnabledConnectionCheckerSettings): T =
    withCheckerActorRef(config)(block)
  def withCheckerActorRef[T](config: EnabledConnectionCheckerSettings)(block: ActorRef => T): T = {
    val checker = childActorOf(ConnectionChecker.props(config))
    val res = block(checker)
    system.stop(watch(checker))
    expectTerminated(checker)
    res
  }

}
