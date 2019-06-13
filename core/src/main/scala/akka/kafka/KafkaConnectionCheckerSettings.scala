/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import akka.actor.ActorSystem
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

class KafkaConnectionCheckerSettings private[kafka] (val maxRetries: Int,
                                                     val checkInterval: FiniteDuration,
                                                     val factor: Double,
                                                     val dispatcher: String) {

  private def copy(maxRetries: Int = maxRetries,
                   checkInterval: FiniteDuration = checkInterval,
                   factor: Double = factor,
                   dispatcher: String = dispatcher): KafkaConnectionCheckerSettings =
    new KafkaConnectionCheckerSettings(
      maxRetries,
      checkInterval,
      factor,
      dispatcher
    )

  def withMaxRetries(maxRetries: Int): KafkaConnectionCheckerSettings =
    copy(maxRetries = maxRetries)
  def withFactor(factor: Double): KafkaConnectionCheckerSettings = copy(factor = factor)
  def withDispatcher(dispatcher: String): KafkaConnectionCheckerSettings = copy(dispatcher = dispatcher)

  /** Scala API */
  def withCheckInterval(checkInterval: FiniteDuration): KafkaConnectionCheckerSettings =
    copy(checkInterval = checkInterval)

  /** Java API */
  def withCheckInterval(checkInterval: java.time.Duration): KafkaConnectionCheckerSettings =
    copy(checkInterval = checkInterval.asScala)

  def getMaxRetries: Int = maxRetries
  def getCheckInterval: java.time.Duration = checkInterval.asJava
  def getFactor: Double = factor
}

object KafkaConnectionCheckerSettings {

  val configPath: String = "connection-checker"
  val fullConfigPath: String = ConsumerSettings.configPath + "." + configPath

  def apply(maxRetries: Int,
            checkInterval: FiniteDuration,
            factor: Double,
            dispatcher: String): KafkaConnectionCheckerSettings = {
    require(factor > 0, "Backoff factor for connection checker must be finite positive number")
    require(maxRetries >= 0, "retries for connection checker must be not negative number")
    new KafkaConnectionCheckerSettings(maxRetries, checkInterval, factor, dispatcher)
  }

  def apply(config: Config): KafkaConnectionCheckerSettings = {
    val retries = config.getInt("max-retries")
    val factor = config.getDouble("backoff-factor")
    val checkInterval = config.getDuration("check-interval").asScala
    val dispatcher = config.getString("dispatcher")
    apply(retries, checkInterval, factor, dispatcher)
  }

  def apply(system: ActorSystem): KafkaConnectionCheckerSettings =
    apply(system.settings.config.getConfig(fullConfigPath))

  def create(maxRetries: Int,
             checkInterval: FiniteDuration,
             factor: Double,
             dispatcher: String): KafkaConnectionCheckerSettings = apply(maxRetries, checkInterval, factor, dispatcher)

  def create(config: Config): KafkaConnectionCheckerSettings = apply(config)

  def create(system: ActorSystem): KafkaConnectionCheckerSettings = apply(system)

}
