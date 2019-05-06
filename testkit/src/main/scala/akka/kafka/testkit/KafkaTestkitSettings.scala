/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit

import akka.actor.ActorSystem
import com.typesafe.config.Config

import scala.concurrent.duration._

class KafkaTestkitSettings private (val clusterTimeout: FiniteDuration,
                                    val consumerGroupTimeout: FiniteDuration,
                                    val consumerSummaryTimeout: FiniteDuration) {

  /**
   * Java Api
   */
  def getClusterTimeout(): java.time.Duration = java.time.Duration.ofMillis(clusterTimeout.toMillis)

  /**
   * Java Api
   */
  def getConsumerGroupTimeout(): java.time.Duration = java.time.Duration.ofMillis(consumerGroupTimeout.toMillis)

  /**
   * Java Api
   */
  def getConsumerSummaryTimeout(): java.time.Duration = java.time.Duration.ofMillis(consumerSummaryTimeout.toMillis)
}

object KafkaTestkitSettings {
  final val ConfigPath = "akka.kafka.testkit"

  /**
   * Create testkit settings from ActorSystem settings.
   */
  def apply(system: ActorSystem): KafkaTestkitSettings =
    KafkaTestkitSettings(system.settings.config.getConfig(ConfigPath))

  /**
   * Java Api
   *
   * Create testkit settings from ActorSystem settings.
   */
  def create(system: ActorSystem): KafkaTestkitSettings = KafkaTestkitSettings(system)

  /**
   * Create testkit settings from a Config.
   */
  def apply(config: Config): KafkaTestkitSettings = {
    val clusterTimeout = config.getDuration("cluster-timeout").toMillis.millis
    val consumerGroupTimeout = config.getDuration("consumer-group-timeout").toMillis.millis
    val consumerSummaryTimeout = config.getDuration("consumer-summary-timeout").toMillis.millis

    new KafkaTestkitSettings(clusterTimeout, consumerGroupTimeout, consumerSummaryTimeout)
  }

  /**
   * Java Api
   *
   * Create testkit settings from a Config.
   */
  def create(config: Config): KafkaTestkitSettings = KafkaTestkitSettings(config)
}
