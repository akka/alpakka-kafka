/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

sealed abstract class ConnectionCheckerSettings private[kafka] (val enable: Boolean)

object ConnectionCheckerSettings {

  val configPath: String = "connection-checker"
  val fullConfigPath: String = ConsumerSettings.configPath + "." + configPath

  //TODO: add description about layer
  def apply(config: Config): ConnectionCheckerSettings = {
    val enable = if (config.hasPath("enable")) config.getBoolean("enable") else false
    if (enable) EnabledConnectionCheckerSettings(config) else DisabledConnectionCheckerSettings
  }

  //TODO: add description about layer
  def create(config: Config): ConnectionCheckerSettings = apply(config)

}

class EnabledConnectionCheckerSettings private[kafka] (val maxRetries: Int,
                                                       val checkInterval: FiniteDuration,
                                                       val factor: Double)
    extends ConnectionCheckerSettings(true) {

  private def copy(maxRetries: Int = maxRetries,
                   checkInterval: FiniteDuration = checkInterval,
                   factor: Double = factor): EnabledConnectionCheckerSettings =
    new EnabledConnectionCheckerSettings(
      maxRetries,
      checkInterval,
      factor
    )

  def withMaxRetries(maxRetries: Int): EnabledConnectionCheckerSettings =
    copy(maxRetries = maxRetries)
  def withFactor(factor: Double): EnabledConnectionCheckerSettings = copy(factor = factor)

  /** Scala API */
  def withCheckInterval(checkInterval: FiniteDuration): EnabledConnectionCheckerSettings =
    copy(checkInterval = checkInterval)

  /** Java API */
  def withCheckInterval(checkInterval: java.time.Duration): EnabledConnectionCheckerSettings =
    copy(checkInterval = checkInterval.asScala)

  override def toString: String =
    s"akka.kafka.EnabledConnectionCheckerSettings(" +
    s"enable=$enable" +
    s"maxRetries=$maxRetries," +
    s"checkInterval=${checkInterval.toCoarsest}," +
    s"factor=$factor" +
    ")"
}

object EnabledConnectionCheckerSettings {

  def apply(maxRetries: Int, checkInterval: FiniteDuration, factor: Double): EnabledConnectionCheckerSettings = {
    require(factor > 0, "Backoff factor for connection checker must be finite positive number")
    require(maxRetries >= 0, "retries for connection checker must be not negative number")
    new EnabledConnectionCheckerSettings(maxRetries, checkInterval, factor)
  }

  def create(maxRetries: Int, checkInterval: FiniteDuration, factor: Double): EnabledConnectionCheckerSettings =
    apply(maxRetries, checkInterval, factor)

  def apply(config: Config): EnabledConnectionCheckerSettings = {
    val retries = config.getInt("max-retries")
    val factor = config.getDouble("backoff-factor")
    val checkInterval = config.getDuration("check-interval").asScala
    apply(retries, checkInterval, factor)
  }

  def creat(config: Config): EnabledConnectionCheckerSettings = apply(config)

}

object DisabledConnectionCheckerSettings extends ConnectionCheckerSettings(false) {

  def getInstance: DisabledConnectionCheckerSettings.type = this

  override def toString: String =
    "akka.kafka.DisabledConnectionCheckerSettings(" +
    s"enable=$enable" +
    ")"

}
