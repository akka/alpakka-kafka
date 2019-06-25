/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

class ConnectionCheckerSettings private[kafka] (val maxRetries: Int,
                                                val checkInterval: FiniteDuration,
                                                val factor: Double) {

  private def copy(maxRetries: Int = maxRetries,
                   checkInterval: FiniteDuration = checkInterval,
                   factor: Double = factor): ConnectionCheckerSettings =
    new ConnectionCheckerSettings(
      maxRetries,
      checkInterval,
      factor
    )

  def withMaxRetries(maxRetries: Int): ConnectionCheckerSettings =
    copy(maxRetries = maxRetries)
  def withFactor(factor: Double): ConnectionCheckerSettings = copy(factor = factor)

  /** Scala API */
  def withCheckInterval(checkInterval: FiniteDuration): ConnectionCheckerSettings =
    copy(checkInterval = checkInterval)

  /** Java API */
  def withCheckInterval(checkInterval: java.time.Duration): ConnectionCheckerSettings =
    copy(checkInterval = checkInterval.asScala)

  override def toString: String =
    s"akka.kafka.ConnectionCheckerSettings(" +
    s"maxRetries=$maxRetries," +
    s"checkInterval=${checkInterval.toCoarsest}," +
    s"factor=$factor" +
    ")"
}

object ConnectionCheckerSettings {

  val configPath: String = "connection-checker"
  val fullConfigPath: String = ConsumerSettings.configPath + "." + configPath

  def apply(maxRetries: Int, checkInterval: FiniteDuration, factor: Double): ConnectionCheckerSettings = {
    require(factor > 0, "Backoff factor for connection checker must be finite positive number")
    require(maxRetries >= 0, "retries for connection checker must be not negative number")
    new ConnectionCheckerSettings(maxRetries, checkInterval, factor)
  }

  //TODO: add description about layer
  def apply(config: Config): ConnectionCheckerSettings = {
    val retries = config.getInt("max-retries")
    val factor = config.getDouble("backoff-factor")
    val checkInterval = config.getDuration("check-interval").asScala
    apply(retries, checkInterval, factor)
  }

  def create(maxRetries: Int, checkInterval: FiniteDuration, factor: Double): ConnectionCheckerSettings =
    apply(maxRetries, checkInterval, factor)

  //TODO: add description about layer
  def create(config: Config): ConnectionCheckerSettings = apply(config)

}
