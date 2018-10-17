/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import akka.kafka.internal.KafkaTestKit
import org.slf4j.{Logger, LoggerFactory}

abstract class KafkaTest extends KafkaTestKit {

  val log: Logger = LoggerFactory.getLogger(getClass)

}
