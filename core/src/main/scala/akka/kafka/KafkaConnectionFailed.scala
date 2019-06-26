package akka.kafka

import org.apache.kafka.common.errors.TimeoutException

final case class KafkaConnectionFailed(te: TimeoutException, attempts: Int)
    extends Exception(s"Can't establish connection with kafkaBroker after $attempts attempts", te)
