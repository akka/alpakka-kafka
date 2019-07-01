/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

import java.time.{Duration => JDuration}

class ConnectionCheckerSettings private[kafka] (val enable: Boolean,
                                                val maxRetries: Int,
                                                val checkInterval: FiniteDuration,
                                                val factor: Double) {

  private def copy(enable: Boolean = enable,
                   maxRetries: Int = maxRetries,
                   checkInterval: FiniteDuration = checkInterval,
                   factor: Double = factor): ConnectionCheckerSettings =
    new ConnectionCheckerSettings(
      enable,
      maxRetries,
      checkInterval,
      factor
    )

  def withEnable(enable: Boolean): ConnectionCheckerSettings = copy(enable = enable)
  def withMaxRetries(maxRetries: Int): ConnectionCheckerSettings = copy(maxRetries = maxRetries)
  def withFactor(factor: Double): ConnectionCheckerSettings = copy(factor = factor)

  /** Scala API */
  def withCheckInterval(checkInterval: FiniteDuration): ConnectionCheckerSettings = copy(checkInterval = checkInterval)

  /** Java API */
  def withCheckInterval(checkInterval: JDuration): ConnectionCheckerSettings =
    copy(checkInterval = checkInterval.asScala)

  override def toString: String =
    s"akka.kafka.ConnectionCheckerSettings(" +
    s"enable=$enable," +
    s"maxRetries=$maxRetries," +
    s"checkInterval=${checkInterval.toCoarsest}," +
    s"factor=$factor" +
    ")"
}

object ConnectionCheckerSettings {

  val configPath: String = "connection-checker"
  val fullConfigPath: String = ConsumerSettings.configPath + "." + configPath

  def apply(enable: Boolean,
            maxRetries: Int,
            checkInterval: FiniteDuration,
            factor: Double): ConnectionCheckerSettings = {
    require(factor > 0, "Backoff factor for connection checker must be finite positive number")
    require(maxRetries >= 0, "retries for connection checker must be not negative number")
    new ConnectionCheckerSettings(enable, maxRetries, checkInterval, factor)
  }

  def create(enable: Boolean,
             maxRetries: Int,
             checkInterval: FiniteDuration,
             factor: Double): ConnectionCheckerSettings =
    apply(enable, maxRetries, checkInterval, factor)

  /**
   * Create settings from a configuration with layout `connection-checker`.
   */
  def apply(config: Config): ConnectionCheckerSettings = {
    val enable = config.getBoolean("enable")
    val retries = config.getInt("max-retries")
    val factor = config.getDouble("backoff-factor")
    val checkInterval = config.getDuration("check-interval").asScala
    apply(enable, retries, checkInterval, factor)
  }

  /**
   * Java API: Create settings from a configuration with layout `connection-checker`.
   */
  def create(config: Config): ConnectionCheckerSettings = apply(config)

}
