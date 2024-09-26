/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka
import java.time.{Duration => JDuration}

import akka.annotation.InternalApi
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.jdk.DurationConverters._

class OffsetResetProtectionSettings @InternalApi private[kafka] (val enable: Boolean,
                                                                 val offsetThreshold: Long,
                                                                 val timeThreshold: FiniteDuration) {
  require(offsetThreshold > 0, "An offset threshold must be greater than 0")
  require(timeThreshold.toMillis > 0, "A time threshold must be greater than 0")

  private def copy(enable: Boolean = enable,
                   offsetThreshold: Long = offsetThreshold,
                   timeThreshold: FiniteDuration = timeThreshold): OffsetResetProtectionSettings = {
    new OffsetResetProtectionSettings(enable, offsetThreshold, timeThreshold)
  }

  /**
   * Whether offset-reset protection should be enabled.
   */
  def withEnable(enable: Boolean): OffsetResetProtectionSettings = copy(enable = enable)

  /**
   * If consumer gets a record with an offset that is more than this number of offsets back from the previously
   * requested offset, it is considered a reset.
   */
  def withOffsetThreshold(offsetThreshold: Long): OffsetResetProtectionSettings =
    copy(offsetThreshold = offsetThreshold)

  /**
   * Scala API.
   *
   * If the record is more than this duration earlier the last received record, it is considered a reset
   */
  def withTimeThreshold(timeThreshold: FiniteDuration): OffsetResetProtectionSettings =
    copy(timeThreshold = timeThreshold)

  /**
   * Java API
   *
   * If the record is more than this duration earlier the last received record, it is considered a reset
   */
  def withTimeThreshold(timeThreshold: JDuration): OffsetResetProtectionSettings =
    copy(timeThreshold = timeThreshold.toScala)

  override def toString: String =
    s"akka.kafka.OffsetResetProtectionSettings(" +
    s"enable=$enable," +
    s"offsetThreshold=$offsetThreshold," +
    s"timeThreshold=${timeThreshold.toCoarsest}" +
    ")"
}

/**
 * The thresholds after which reset protection is enabled. Offsets, time, or both can be provided.
 */
object OffsetResetProtectionSettings {

  val configPath: String = "offset-reset-protection"

  /**
   * Enable offset-reset protection with the given offset and time threshold, where offsets received outside the
   * threshold are considered indicative of an offset reset.
   */
  def apply(offsetThreshold: Long, timeThreshold: FiniteDuration): OffsetResetProtectionSettings =
    new OffsetResetProtectionSettings(true, offsetThreshold, timeThreshold)

  /**
   * Enable offset-reset protection with the given offset and time threshold, where offsets received outside the
   * threshold are considered indicative of an offset reset.
   */
  def apply(offsetThreshold: Long, timeThreshold: java.time.Duration): OffsetResetProtectionSettings =
    new OffsetResetProtectionSettings(true, offsetThreshold, timeThreshold.toScala)

  /**
   * Create settings from a configuration with layout `connection-checker`.
   */
  def apply(config: Config): OffsetResetProtectionSettings = {
    val enable = config.getBoolean("enable")
    if (enable) {
      val offsetThreshold = config.getLong("offset-threshold")
      val timeThreshold = config.getDuration("time-threshold").toScala
      apply(offsetThreshold, timeThreshold)
    } else Disabled
  }

  /**
   * Java API: Create settings from a configuration with layout `offset-reset-protection`.
   */
  def create(config: Config): OffsetResetProtectionSettings = apply(config)

  // max duration is ~292 years, so we are a touch below that to be safe
  val Disabled: OffsetResetProtectionSettings = new OffsetResetProtectionSettings(false, Long.MaxValue, 100000.days)
}
