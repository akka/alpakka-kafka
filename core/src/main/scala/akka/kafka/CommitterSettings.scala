/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import scala.concurrent.duration._

object CommitterSettings {

  val configPath = "akka.kafka.committer"

  /**
   * Create settings from the default configuration
   * `akka.kafka.committer`.
   */
  def apply(actorSystem: ActorSystem): CommitterSettings =
    apply(actorSystem.settings.config.getConfig(configPath))

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.committer`.
   */
  def apply(config: Config): CommitterSettings = {
    val maxBatch = config.getLong("max-batch")
    val maxInterval = config.getDuration("max-interval", TimeUnit.MILLISECONDS).millis
    val parallelism = config.getInt("parallelism")
    new CommitterSettings(maxBatch, maxInterval, parallelism)
  }

  /**
   * Java API: Create settings from the default configuration
   * `akka.kafka.committer`.
   */
  def create(actorSystem: ActorSystem): CommitterSettings =
    apply(actorSystem)

  /**
   * Java API: Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.committer`.
   */
  def create(config: Config): CommitterSettings =
    apply(config)

}

/**
 * Settings for committer. See `akka.kafka.committer` section in
 * reference.conf. Note that the [[akka.kafka.CommitterSettings$ companion]] object provides
 * `apply` and `create` functions for convenient construction of the settings, together with
 * the `with` methods.
 */
class CommitterSettings private (
    val maxBatch: Long,
    val maxInterval: FiniteDuration,
    val parallelism: Int
) {

  def withMaxBatch(maxBatch: Long): CommitterSettings =
    copy(maxBatch = maxBatch)

  def withMaxInterval(maxInterval: FiniteDuration): CommitterSettings =
    copy(maxInterval = maxInterval)

  def withMaxInterval(maxInterval: java.time.Duration): CommitterSettings =
    copy(maxInterval = maxInterval.asScala)

  def withParallelism(parallelism: Int): CommitterSettings =
    copy(parallelism = parallelism)

  private def copy(maxBatch: Long = maxBatch,
                   maxInterval: FiniteDuration = maxInterval,
                   parallelism: Int = parallelism): CommitterSettings =
    new CommitterSettings(maxBatch, maxInterval, parallelism)

  override def toString: String =
    "akka.kafka.CommitterSettings(" +
    s"maxBatch=$maxBatch," +
    s"maxInterval=${maxInterval.toCoarsest}," +
    s"parallelism=$parallelism" +
    ")"

}
