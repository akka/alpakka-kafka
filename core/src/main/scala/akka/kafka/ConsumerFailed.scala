/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

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
