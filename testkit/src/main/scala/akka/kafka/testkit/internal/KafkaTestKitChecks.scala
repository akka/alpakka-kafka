/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.internal

import java.util.Collections
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.admin.{
  Admin,
  ConsumerGroupDescription,
  DescribeClusterResult,
  DescribeConsumerGroupsOptions
}
import org.slf4j.Logger

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

object KafkaTestKitChecks {
  def waitUntilCluster(timeout: FiniteDuration,
                       sleepInBetween: FiniteDuration,
                       adminClient: Admin,
                       predicate: DescribeClusterResult => Boolean,
                       log: Logger): Unit =
    periodicalCheck("cluster state", timeout, sleepInBetween)(() => adminClient.describeCluster())(predicate)(log)

  def waitUntilConsumerGroup(groupId: String,
                             timeout: FiniteDuration,
                             sleepInBetween: FiniteDuration,
                             adminClient: Admin,
                             predicate: ConsumerGroupDescription => Boolean,
                             log: Logger): Unit =
    periodicalCheck("consumer group state", timeout, sleepInBetween)(
      () =>
        adminClient
          .describeConsumerGroups(
            Collections.singleton(groupId),
            new DescribeConsumerGroupsOptions().timeoutMs(timeout.toMillis.toInt)
          )
          .describedGroups()
          .get(groupId)
          .get(timeout.toMillis, TimeUnit.MILLISECONDS)
    )(predicate)(log)

  def periodicalCheck[T](description: String, timeout: FiniteDuration, sleepInBetween: FiniteDuration)(
      data: () => T
  )(predicate: T => Boolean)(log: Logger): Unit = {
    val maxTries = (timeout / sleepInBetween).toInt

    @tailrec def check(triesLeft: Int): Unit =
      Try(predicate(data())).recover {
        case ex =>
          log.debug(s"Ignoring [${ex.getClass.getName}: ${ex.getMessage}] while waiting for desired state")
          false
      } match {
        case Success(false) if triesLeft > 0 =>
          Thread.sleep(sleepInBetween.toMillis)
          check(triesLeft - 1)
        case Success(false) =>
          throw new Error(
            s"Timeout while waiting for desired $description. Tried [$maxTries] times, slept [$sleepInBetween] in between."
          )
        case Failure(ex) =>
          throw ex
        case Success(true) => // predicate has been fulfilled, stop checking
      }

    check(maxTries)
  }
}
