/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka
import java.util.concurrent.TimeUnit

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

@ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1092")
sealed trait CommitWhen

/**
 * Selects when the stream will commit an offset.
 */
@ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1092")
object CommitWhen {

  /**
   * Commit as soon as a [[Committable]] is observed.
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1092")
  case object FirstObserved extends CommitWhen

  /**
   * Commit once a new [[Committable]] is observed
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1092")
  case object DeferToNextOffset extends CommitWhen

  /**
   * Java API.
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1092")
  val firstObserved: CommitWhen = FirstObserved

  /**
   * Java API.
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1092")
  val deferToNextOffset: CommitWhen = DeferToNextOffset

  def valueOf(s: String): CommitWhen = s match {
    case "FirstObserved" => FirstObserved
    case "DeferToNextOffset" => DeferToNextOffset
    case other =>
      throw new IllegalArgumentException(s"allowed values are: FirstObserved, DeferToNextOffset. Received: $other")
  }
}

object CommitterSettings {

  val configPath = "akka.kafka.committer"

  /**
   * Create settings from the default configuration
   * `akka.kafka.committer`.
   */
  def apply(actorSystem: akka.actor.ActorSystem): CommitterSettings =
    apply(actorSystem.settings.config.getConfig(configPath))

  /**
   * Create settings from the default configuration
   * `akka.kafka.committer`.
   *
   * For use with the `akka.actor.typed` API.
   */
  def apply(actorSystem: akka.actor.ClassicActorSystemProvider): CommitterSettings =
    apply(actorSystem.classicSystem.settings.config.getConfig(configPath))

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.committer`.
   */
  def apply(config: Config): CommitterSettings = {
    val maxBatch = config.getLong("max-batch")
    val maxInterval = config.getDuration("max-interval", TimeUnit.MILLISECONDS).millis
    val parallelism = config.getInt("parallelism")
    val delivery = CommitDelivery.valueOf(config.getString("delivery"))
    val when = CommitWhen.valueOf(config.getString("when"))
    new CommitterSettings(maxBatch, maxInterval, parallelism, delivery, when)
  }

  /**
   * Java API: Create settings from the default configuration
   * `akka.kafka.committer`.
   */
  def create(actorSystem: akka.actor.ActorSystem): CommitterSettings =
    apply(actorSystem)

  /**
   * Java API: Create settings from the default configuration
   * `akka.kafka.committer`.
   *
   * For use with the `akka.actor.typed` API.
   */
  def create(actorSystem: akka.actor.ClassicActorSystemProvider): CommitterSettings =
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
    val when: CommitWhen
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

  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1092")
  def withCommitWhen(value: CommitWhen): CommitterSettings =
    copy(when = value)

  private def copy(maxBatch: Long = maxBatch,
                   maxInterval: FiniteDuration = maxInterval,
                   parallelism: Int = parallelism,
                   delivery: CommitDelivery = delivery,
                   when: CommitWhen = when): CommitterSettings =
    new CommitterSettings(maxBatch, maxInterval, parallelism, delivery, when)

  override def toString: String =
    "akka.kafka.CommitterSettings(" +
    s"maxBatch=$maxBatch," +
    s"maxInterval=${maxInterval.toCoarsest}," +
    s"parallelism=$parallelism," +
    s"delivery=$delivery," +
    s"when=$when)"
}
