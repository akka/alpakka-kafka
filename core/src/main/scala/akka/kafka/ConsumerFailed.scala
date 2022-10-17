/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka

import scala.concurrent.duration.FiniteDuration

/**
 * Kafka consumer stages fail with this exception.
 */
class ConsumerFailed(msg: String) extends RuntimeException(msg) {
  def this() = this("Consumer actor terminated") // for backwards compatibility
  def this(cause: Throwable) = {
    this()
    initCause(cause)
  }
  def this(msg: String, cause: Throwable) = {
    this(msg)
    initCause(cause)
  }
}

class InitialPollFailed(val timeout: Long, val bootstrapServers: String)
    extends ConsumerFailed(
      s"Initial consumer poll($timeout) with bootstrap servers " +
      s"$bootstrapServers did not succeed, correct address?"
    )

class WakeupsExceeded(val timeout: Long, val maxWakeups: Int, val wakeupTimeout: FiniteDuration)
    extends ConsumerFailed(
      s"WakeupException limit exceeded during poll($timeout), stopping (max-wakeups = $maxWakeups, wakeup-timeout = ${wakeupTimeout.toCoarsest})."
    )
