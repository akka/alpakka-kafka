/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import org.apache.kafka.common.errors.TimeoutException

final case class KafkaConnectionFailed(te: TimeoutException, attempts: Int)
    extends Exception(s"Can't establish connection with kafkaBroker after $attempts attempts", te)
