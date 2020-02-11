/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import scala.concurrent.duration._

@ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/882")
sealed trait CommitDelivery

/**
 * Selects how the stream delivers commits to the internal actor.
 */
@ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/882")
object CommitDelivery {

  /**
   * Expect replies for commits, and backpressure the stream if replies do not
   * arrive.
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/882")
  case object WaitForAck extends CommitDelivery

  /**
   * Send off commits to the internal actor without expecting replies,
   * and don't create backpressure in the stream.
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/882")
  case object SendAndForget extends CommitDelivery

  /**
   * Java API.
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/882")
  val waitForAck: CommitDelivery = WaitForAck

  /**
   * Java API.
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/882")
  val sendAndForget: CommitDelivery = SendAndForget

  def valueOf(s: String): CommitDelivery = s match {
    case "WaitForAck" => WaitForAck
    case "SendAndForget" => SendAndForget
    case other => throw new IllegalArgumentException(s"allowed values are: WaitForAck, SendAndForget. Received: $other")
  }
}

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
    val delivery = CommitDelivery.valueOf(config.getString("delivery"))
    val flushPartialBatch = config.getBoolean("flush-partial-batches")
    new CommitterSettings(maxBatch, maxInterval, parallelism, delivery, flushPartialBatch)
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
    val parallelism: Int,
    val delivery: CommitDelivery,
    val flushPartialBatches: Boolean
) {

  def withMaxBatch(maxBatch: Long): CommitterSettings =
    copy(maxBatch = maxBatch)

  def withMaxInterval(maxInterval: FiniteDuration): CommitterSettings =
    copy(maxInterval = maxInterval)

  def withMaxInterval(maxInterval: java.time.Duration): CommitterSettings =
    copy(maxInterval = maxInterval.asScala)

  def withParallelism(parallelism: Int): CommitterSettings =
    copy(parallelism = parallelism)

  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/882")
  def withDelivery(value: CommitDelivery): CommitterSettings =
    copy(delivery = value)

  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1039")
  def withFlushPartialBatches(flushPartialBatches: Boolean): CommitterSettings =
    copy(flushPartialBatches = flushPartialBatches)

  private def copy(maxBatch: Long = maxBatch,
                   maxInterval: FiniteDuration = maxInterval,
                   parallelism: Int = parallelism,
                   delivery: CommitDelivery = delivery,
                   flushPartialBatches: Boolean = flushPartialBatches): CommitterSettings =
    new CommitterSettings(maxBatch, maxInterval, parallelism, delivery, flushPartialBatches)

  override def toString: String =
    "akka.kafka.CommitterSettings(" +
    s"maxBatch=$maxBatch," +
    s"maxInterval=${maxInterval.toCoarsest}," +
    s"parallelism=$parallelism," +
    s"delivery=$delivery," +
    s"flushPartialBatches=$flushPartialBatches)"
}
