/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka
import java.time.{Duration => JDuration}

import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import scala.concurrent.duration._

class ResetProtectionSettings private[kafka] (val enable: Boolean,
                                              val offsetThreshold: Long,
                                              val timeThreshold: FiniteDuration) {
  require(offsetThreshold > 0, "A offset threshold must be greater than 0")
  require(timeThreshold.toMillis > 0, "A time threshold must be greater than 0")

  private def copy(enable: Boolean = enable,
                   offsetThreshold: Long = offsetThreshold,
                   timeThreshold: FiniteDuration = timeThreshold): ResetProtectionSettings = {
    new ResetProtectionSettings(enable, offsetThreshold, timeThreshold)
  }

  def withEnable(enable: Boolean): ResetProtectionSettings = copy(enable = enable)
  def withOffsetThreshold(offsetThreshold: Long): ResetProtectionSettings = copy(offsetThreshold = offsetThreshold)

  /** Scala API */
  def withTimeThreshold(timeThreshold: FiniteDuration): ResetProtectionSettings = copy(timeThreshold = timeThreshold)

  /** Java API */
  def withTimeThreshold(timeThreshold: JDuration): ResetProtectionSettings =
    copy(timeThreshold = timeThreshold.asScala)

  override def toString: String =
    s"akka.kafka.ResetProtectionSettings(" +
    s"enable=$enable," +
    s"offsetThreshold=$offsetThreshold," +
    s"timeThreshold=${timeThreshold.toCoarsest}" +
    ")"
}

/**
 * The thresholds after which reset protection is enabled. Offsets, time, or both can be provided.
 */
object ResetProtectionSettings {

  val configPath: String = "reset-protection"

  def apply(offsetThreshold: Long, timeThreshold: FiniteDuration): ResetProtectionSettings =
    new ResetProtectionSettings(true, offsetThreshold, timeThreshold)

  /**
   * Create settings from a configuration with layout `connection-checker`.
   */
  def apply(config: Config): ResetProtectionSettings = {
    val enable = config.getBoolean("enable")
    if (enable) {
      val offsetThreshold = config.getLong("offset-threshold")
      val timeThreshold = config.getDuration("time-threshold").asScala
      apply(offsetThreshold, timeThreshold)
    } else Disabled
  }

  /**
   * Java API: Create settings from a configuration with layout `reset-protection`.
   */
  def create(config: Config): ResetProtectionSettings = apply(config)

  // max duration is ~292 years, so we are a touch below that to be safe
  val Disabled: ResetProtectionSettings = new ResetProtectionSettings(false, Long.MaxValue, 100000.days)
}
